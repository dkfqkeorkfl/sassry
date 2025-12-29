use std::{collections::HashMap, fmt, sync::Arc};

use mongodb::{
    bson::{doc, Document},
    Client, Collection,
};
use serde::{Deserialize, Serialize, Serializer};
use tower_cookies::cookie::time::OffsetDateTime;
use tower_sessions::{
    session::{Id, Record as TowerRecord},
    session_store::Error as SessionError,
    SessionStore,
};

use cassry::*;

/// MongoDB에 저장할 Record 구조체 (역직렬화용)
/// id를 _id로 rename하여 MongoDB의 _id 필드와 매핑
#[derive(Debug, Clone, Deserialize, PartialEq)]
struct MongoRecord {
    #[serde(rename = "_id")]
    pub id: Id,
    pub data: HashMap<String, serde_json::Value>,
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
struct MongoRecordRef<'a> {
    id: &'a Id,
    data: &'a HashMap<String, serde_json::Value>,
    expiry_date: &'a OffsetDateTime,
}

impl<'a> From<&'a TowerRecord> for MongoRecordRef<'a> {
    fn from(record: &'a TowerRecord) -> Self {
        Self {
            id: &record.id,
            data: &record.data,
            expiry_date: &record.expiry_date,
        }
    }
}

impl<'a> From<&'a mut TowerRecord> for MongoRecordRef<'a> {
    fn from(record: &'a mut TowerRecord) -> Self {
        Self {
            id: &record.id,
            data: &record.data,
            expiry_date: &record.expiry_date,
        }
    }
}

impl<'a> Serialize for MongoRecordRef<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("MongoRecord", 3)?;
        state.serialize_field("_id", self.id)?;
        state.serialize_field("data", self.data)?;
        state.serialize_field("expiry_date", self.expiry_date)?;
        state.end()
    }
}

/// MongoDB를 사용하는 세션 저장소 구현
#[derive(Clone)]
pub struct MongoSessionStore {
    collection: Arc<Collection<Document>>,
}

impl fmt::Debug for MongoSessionStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("MongoSessionStore")
    }
}

impl MongoSessionStore {
    /// MongoDB 클라이언트와 데이터베이스 이름, 컬렉션 이름을 받아서 초기화
    pub fn new(client: Client, database_name: &str, collection_name: &str) -> Self {
        let database = client.database(database_name);
        let collection = database.collection::<Document>(collection_name);
        Self {
            collection: Arc::new(collection),
        }
    }
}

#[async_trait::async_trait]
impl SessionStore for MongoSessionStore {
    async fn create(&self, record: &mut TowerRecord) -> Result<(), SessionError> {
        let result = async {
            // TowerRecord 레퍼런스를 MongoRecordRef로 변환 후 Document로 직렬화
            let mongo_record_ref: MongoRecordRef = record.into();
            let doc = mongodb::bson::to_document(&mongo_record_ref)
                .map_err(|e| SessionError::Encode(e.to_string()))?;

            // MongoDB에 삽입
            self.collection
                .insert_one(doc)
                .await
                .map_err(|e| SessionError::Backend(e.to_string()))?;

            Ok::<_, SessionError>(())
        }
        .await;

        match result {
            Ok(_) => Ok(()),
            Err(e) => {
                error!("create session error: {}", e.to_string());
                Err(e)
            }
        }
    }

    async fn save(&self, record: &TowerRecord) -> Result<(), SessionError> {
        let result = async {
            // TowerRecord 레퍼런스를 MongoRecordRef로 변환 후 Document로 직렬화
            let mongo_record_ref: MongoRecordRef = record.into();
            let doc = mongodb::bson::to_document(&mongo_record_ref)
                .map_err(|e| SessionError::Encode(e.to_string()))?;

            let filter = doc! { "_id": record.id.to_string() };
            let update = doc! { "$set": doc };

            self.collection
                .update_one(filter, update)
                .await
                .map_err(|e| SessionError::Backend(e.to_string()))?;

            Ok::<_, SessionError>(())
        }
        .await;

        match result {
            Ok(_) => Ok(()),
            Err(e) => {
                error!("save session error: {}", e.to_string());
                Err(e)
            }
        }
    }

    async fn load(&self, session_id: &Id) -> Result<Option<TowerRecord>, SessionError> {
        let result: Result<Option<TowerRecord>, SessionError> = async {
            // MongoDB에서 조회
            let filter = doc! { "_id": session_id.to_string() };
            let doc = self
                .collection
                .find_one(filter)
                .await
                .map_err(|e| SessionError::Backend(e.to_string()))?;

            if let Some(doc) = doc {
                // Document를 MongoRecord로 역직렬화 후 TowerRecord로 변환
                let mongo_record: MongoRecord = mongodb::bson::from_document(doc)
                    .map_err(|e| SessionError::Decode(e.to_string()))?;

                let record: TowerRecord = mongo_record.into();
                Ok(Some(record))
            } else {
                Ok(None)
            }
        }
        .await;

        match result {
            Ok(v) => Ok(v),
            Err(e) => {
                error!("load session error: {}", e.to_string());
                Err(e)
            }
        }
    }

    async fn delete(&self, session_id: &Id) -> Result<(), SessionError> {
        let result = async {
            // MongoDB에서 삭제 (MongoDB 3.0에서는 delete_one이 1개 인자만 받음)
            let filter = doc! { "_id": session_id.to_string() };
            self.collection
                .delete_one(filter)
                .await
                .map_err(|e| SessionError::Backend(e.to_string()))?;

            Ok::<_, SessionError>(())
        }
        .await;

        match result {
            Ok(_) => Ok(()),
            Err(e) => {
                error!("delete session error: {}", e.to_string());
                Err(e)
            }
        }
    }
}
