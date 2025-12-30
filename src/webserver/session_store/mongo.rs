use std::{collections::HashMap, fmt, sync::Arc};

use bson::serde_helpers::datetime::FromTime03OffsetDateTime;
use cassry::*;
use mongodb::{
    bson::{doc, Document},
    Client as MongoClient, Collection, Database,
};
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr};
use tower_cookies::cookie::time::OffsetDateTime;
use tower_sessions::{
    session::{Id, Record as TowerRecord},
    session_store::Error as SessionError,
    SessionStore,
};

#[derive(Clone)]
pub struct Client {
    client: MongoClient,
    database: Database,
}

impl Client {
    pub fn new(client: MongoClient, database_name: &str) -> Self {
        let database = client.database(database_name);
        Self { client, database }
    }

    pub fn get_collection(&self, collection_name: &str) -> Collection<Document> {
        self.database.collection::<Document>(collection_name)
    }

    /// 컬렉션을 반환
    /// 타입을 지정하지 않으면 Document를 반환하며, 제네릭 타입을 지정하여 다른 타입도 사용 가능
    ///
    /// # Examples
    /// ```
    /// // Document 타입 (기본)
    /// let collection = mongo_db.get_collection("tokens");
    ///
    /// // 특정 타입 지정
    /// let collection: Collection<MyStruct> = mongo_db.get_collection("tokens");
    /// ```
    pub fn get_collection_typed<T>(&self, collection_name: &str) -> Collection<T>
    where
        T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync + Unpin,
    {
        self.database.collection::<T>(collection_name)
    }

    /// 내부 Client에 접근 (필요한 경우)
    pub fn client(&self) -> &MongoClient {
        &self.client
    }

    /// 내부 Database에 접근 (필요한 경우)
    pub fn database(&self) -> &Database {
        &self.database
    }
}

/// MongoDB에 저장할 Record 구조체 (역직렬화용)
/// id를 _id로 rename하여 MongoDB의 _id 필드와 매핑
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct MongoRecord {
    #[serde(rename = "_id")]
    #[serde_as(as = "DisplayFromStr")]
    pub id: Id,
    pub data: HashMap<String, serde_json::Value>,
    #[serde(rename = "exp")]
    #[serde_as(as = "FromTime03OffsetDateTime")]
    pub expiry_date: OffsetDateTime,
}

impl Into<TowerRecord> for MongoRecord {
    fn into(self) -> TowerRecord {
        TowerRecord {
            id: self.id,
            data: self.data,
            expiry_date: self.expiry_date,
        }
    }
}

/// MongoDB에 저장할 Record 구조체 (직렬화용 - 레퍼런스)
/// id를 _id로 rename하여 MongoDB의 _id 필드와 매핑
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]

struct RecordHelper {
    #[serde(rename = "_id")]
    #[serde_as(as = "DisplayFromStr")]
    pub id: Id,
    #[serde(rename = "exp")]
    #[serde_as(as = "FromTime03OffsetDateTime")]
    pub expiry_date: OffsetDateTime,
}

/// MongoDB를 사용하는 세션 저장소 구현
#[derive(Clone)]
pub struct MongoSessionStore {
    client: Arc<Client>,
    collection_name: String,
}

impl fmt::Debug for MongoSessionStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("MongoSessionStore")
    }
}

impl MongoSessionStore {
    /// MongoDB 클라이언트와 데이터베이스 이름, 컬렉션 이름을 받아서 초기화
    pub fn new(client: Arc<Client>, collection_name: String) -> Self {
        Self {
            client,
            collection_name,
        }
    }
}

#[async_trait::async_trait]
impl SessionStore for MongoSessionStore {
    async fn create(&self, record: &mut TowerRecord) -> Result<(), SessionError> {
        let mongo_record_for_write = RecordHelper {
            id: record.id,
            expiry_date: record.expiry_date,
        };
        let mut root = bson::ser::serialize_to_document(&mongo_record_for_write)
            .map_err(|e| SessionError::Encode(e.to_string()))?;
        if !record.data.is_empty() {
            let data = bson::ser::serialize_to_document(&record.data)
                .map_err(|e| SessionError::Encode(e.to_string()))?;
            root.insert("data", data);
        }

        self.client
            .get_collection(&self.collection_name)
            .insert_one(root)
            .await
            .map_err(|e| SessionError::Backend(e.to_string()))?;
        Ok(())
    }

    async fn save(&self, record: &TowerRecord) -> Result<(), SessionError> {
        let mongo_record_for_write = RecordHelper {
            id: record.id,
            expiry_date: record.expiry_date,
        };
        let mut root = bson::ser::serialize_to_document(&mongo_record_for_write)
            .map_err(|e| SessionError::Encode(e.to_string()))?;
        let data = bson::ser::serialize_to_document(&record.data)
            .map_err(|e| SessionError::Encode(e.to_string()))?;
        root.insert("data", data);

        let key = root
            .remove("_id")
            .ok_or(SessionError::Encode("Failed to get key".to_string()))?;
        let filter = doc! { "_id": key };
        let update = doc! { "$set": root };
        self.client
            .get_collection(&self.collection_name)
            .update_one(filter, update)
            .await
            .map_err(|e| SessionError::Backend(e.to_string()))?;
        Ok(())
    }

    async fn load(&self, session_id: &Id) -> Result<Option<TowerRecord>, SessionError> {
        let filter = doc! { "_id": session_id.to_string() };
        let doc = self
            .client
            .get_collection_typed::<MongoRecord>(&self.collection_name)
            .find_one(filter)
            .await
            .map_err(|e| SessionError::Backend(e.to_string()))?;

        Ok(doc.map(|doc| doc.into()))
    }

    async fn delete(&self, session_id: &Id) -> Result<(), SessionError> {
        let filter = doc! { "_id": session_id.to_string() };
        self.client
            .get_collection_typed::<MongoRecord>(&self.collection_name)
            .delete_one(filter)
            .await
            .map_err(|e| SessionError::Backend(e.to_string()))?;

        Ok(())
    }
}
