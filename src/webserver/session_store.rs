use cassry::{chrono, serde_json, twdb::TimeWindowDB, *};
use moka::future::{Cache, CacheBuilder};
use std::{fmt, sync::Arc};
use tower_sessions::{
    session::{Id, Record},
    session_store::Error as SeesionError,
    SessionStore,
};

/// TWDB를 사용하는 세션 저장소 구현
#[derive(Clone)]
pub struct FastSessionStore {
    local_cache: Arc<TimeWindowDB>,
    redis: Arc<Cache<String, String>>,
}

impl fmt::Debug for FastSessionStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("HybridSessionStore")
    }
}

impl FastSessionStore {
    pub async fn new(ttl: i64) -> anyhow::Result<Self> {
        let local_cache = Arc::new(
            TimeWindowDB::new(
                "session".to_string(),
                chrono::Duration::seconds(ttl / 2),
                true,
            )
            .await?,
        );
        let local_db = Arc::new(
            CacheBuilder::new(1000)
                .time_to_idle(std::time::Duration::from_secs(ttl as u64))
                .build(),
        );

        Ok(Self {
            local_cache: local_cache,
            redis: local_db,
        })
    }

    fn redis_key(session_id: &Id) -> String {
        format!("session:{}", session_id.to_string())
    }
}

#[async_trait::async_trait]
impl SessionStore for FastSessionStore {
    async fn create(&self, record: &mut Record) -> Result<(), SeesionError> {
        let result = async {
            // 로컬 캐시에 저장
            let session_json =
                serde_json::to_string(record).map_err(|e| SeesionError::Encode(e.to_string()))?;

            self.local_cache
                .put(record.id.to_string(), session_json.clone())
                .await
                .map_err(|e| SeesionError::Backend(e.to_string()))?;

            Ok::<_, SeesionError>(session_json)
        }
        .await;

        match result {
            Ok(session_json) => {
                // moka 캐시에 저장 (신규규)
                let key = Self::redis_key(&record.id);
                self.redis.insert(key, session_json).await;
                Ok(())
            }
            Err(e) => {
                // 세션에서 에러가 나면 response에서 500에러만 반환이 되어 별도 로깅
                error!("create session error: {}", e.to_string());
                Err(e)
            }
        }
    }

    async fn save(&self, record: &Record) -> Result<(), SeesionError> {
        // 로컬 캐시에 저장 (기존 세션 업데이트)

        let result = async {
            let session_json =
                serde_json::to_string(record).map_err(|e| SeesionError::Encode(e.to_string()))?;

            self.local_cache
                .put(record.id.to_string(), session_json.clone())
                .await
                .map_err(|e| SeesionError::Backend(e.to_string()))?;
            Ok::<_, SeesionError>(session_json)
        }
        .await;

        match result {
            Ok(session_json) => {
                // moka 캐시에 저장 (업데이트)
                let key = Self::redis_key(&record.id);
                self.redis.insert(key, session_json).await;
                Ok(())
            }
            Err(e) => {
                // 세션에서 에러가 나면 response에서 500에러만 반환이 되어 별도 로깅
                error!("save session error: {}", e.to_string());
                Err(e)
            }
        }
    }

    async fn load(&self, session_id: &Id) -> Result<Option<Record>, SeesionError> {
        // 1. 로컬 캐시에서 조회
        let result = async {
            if let Some(session_json) = self
                .local_cache
                .get(session_id.to_string())
                .await
                .map_err(|e| SeesionError::Decode(e.to_string()))?
            {
                if let Ok(record) = serde_json::from_str(&session_json) {
                    return Ok(Some(record));
                }
            }

            // 2. moka 캐시에서 조회
            let key = Self::redis_key(session_id);
            if let Some(session_json) = self.redis.get(&key).await {
                // moka 캐시에서 찾았다면 로컬 캐시에도 저장
                if let Ok(record) = serde_json::from_str(&session_json) {
                    self.local_cache
                        .put(session_id.to_string(), session_json)
                        .await
                        .map_err(|e| SeesionError::Backend(e.to_string()))?;
                    return Ok(Some(record));
                }
            }
            Ok::<Option<Record>, SeesionError>(None)
        }
        .await;

        match result {
            Ok(v) => Ok(v),
            Err(e) => {
                // 세션에서 에러가 나면 response에서 500에러만 반환이 되어 별도 로깅
                error!("load session error: {}", e.to_string());
                Err(e)
            }
        }
    }

    async fn delete(&self, session_id: &Id) -> Result<(), SeesionError> {
        // 로컬 캐시에서 삭제
        let result = async {
            self.local_cache
                .delete(session_id.to_string())
                .await
                .map_err(|e| SeesionError::Backend(e.to_string()))?;
            Ok::<_, SeesionError>(())
        }
        .await;

        if let Err(e) = result {
            // 세션에서 에러가 나면 response에서 500에러만 반환이 되어 별도 로깅
            error!("delete session error: {}", e.to_string());
            return Err(e);
        }

        // moka 캐시에서 삭제
        {
            let key = Self::redis_key(session_id);
            self.redis.remove(&key).await;
        }

        Ok(())
    }
}
