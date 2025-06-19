use std::{fmt, sync::Arc};
use tower_sessions::{session::{Id, Record}, SessionStore, session_store::Error as SeesionError};
use cassry::{serde_json, twdb::TimeWindowDB, LocalDB};

/// TWDB를 사용하는 세션 저장소 구현
#[derive(Clone)]
pub struct FastSessionStore {
    local_cache: Arc<TimeWindowDB>,
    redis: LocalDB,
}

impl fmt::Debug for FastSessionStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("HybridSessionStore")
    }
}

impl FastSessionStore {
    pub fn new(local_cache: Arc<TimeWindowDB>, local_db: LocalDB) -> Self {
        Self {
            local_cache: local_cache,
            redis: local_db,
        }
    }

    fn redis_key(session_id: &Id) -> String {
        format!("session:{}", session_id.to_string())
    }
}

#[async_trait::async_trait]
impl SessionStore for FastSessionStore {
    async fn create(&self, record: &mut Record) -> Result<(), SeesionError> {
        // 로컬 캐시에 저장
        let session_json = serde_json::to_string(record)
            .map_err(|e| SeesionError::Encode(e.to_string()))?;
        
        {
            self.local_cache.put(record.id.to_string(), session_json.clone())
                .await
                .map_err(|e| SeesionError::Backend(e.to_string()))?;
        }

        // LocalDB에 저장
        {
            let key = Self::redis_key(&record.id);
            self.redis.put(key, session_json)
                .await
                .map_err(|e| SeesionError::Backend(e.to_string()))?;
        }

        Ok(())
    }

    async fn save(&self, record: &Record) -> Result<(), SeesionError> {
        // 로컬 캐시에 저장 (기존 세션 업데이트)
        let session_json = serde_json::to_string(record)
            .map_err(|e| SeesionError::Encode(e.to_string()))?;
        
        {
            self.local_cache.put(record.id.to_string(), session_json.clone())
                .await
                .map_err(|e| SeesionError::Backend(e.to_string()))?;
        }

        // LocalDB에 저장 (업데이트)
        {
            let key = Self::redis_key(&record.id);
            self.redis.put(key, session_json)
                .await
                .map_err(|e| SeesionError::Backend(e.to_string()))?;
        }

        Ok(())
    }

    async fn load(&self, session_id: &Id) -> Result<Option<Record>, SeesionError> {
        // 1. 로컬 캐시에서 조회
        {
            if let Some(session_json) = self.local_cache.get(session_id.to_string())
                .await
                .map_err(|e| SeesionError::Decode(e.to_string()))? 
            {
                if let Ok(record) = serde_json::from_str(&session_json) {
                    return Ok(Some(record));
                }
            }
        }

        // 2. LocalDB에서 조회
        {
            let key = Self::redis_key(session_id);
            if let Some(session_json) = self.redis.get(key)
                .await
                .map_err(|e| SeesionError::Decode(e.to_string()))?
            {
                // LocalDB에서 찾았다면 로컬 캐시에도 저장
                if let Ok(record) = serde_json::from_str(&session_json) {
                    self.local_cache.put(session_id.to_string(), session_json)
                        .await
                        .map_err(|e| SeesionError::Backend(e.to_string()))?;
                    return Ok(Some(record));
                }
            }
        }

        Ok(None)
    }

    async fn delete(&self, session_id: &Id) -> Result<(), SeesionError> {
        // 로컬 캐시에서 삭제
        {
            self.local_cache.delete(session_id.to_string())
                .await
                .map_err(|e| SeesionError::Backend(e.to_string()))?;
        }

        // LocalDB에서 삭제
        {
            let key = Self::redis_key(session_id);
            self.redis.delete(key)
                .await
                .map_err(|e| SeesionError::Backend(e.to_string()))?;
        }

        Ok(())
    }
}