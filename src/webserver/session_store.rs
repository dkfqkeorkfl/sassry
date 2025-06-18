use std::fmt;
use tower_sessions::{session::{Id, Record}, SessionStore, session_store::Error as SeesionError};
use cassry::{serde_json, twdb::TimeWindowDB};

/// TWDB를 사용하는 세션 저장소 구현
#[derive(Clone)]
pub struct FastSessionStore {
    local_cache: TimeWindowDB,
}

impl fmt::Debug for FastSessionStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("HybridSessionStore")
    }
}

impl FastSessionStore {
    pub fn new(local_cache: TimeWindowDB) -> Self {
        Self {
            local_cache: local_cache
        }
    }

    // fn redis_key(session_id: &Id) -> String {
    //     format!("session:{}", session_id.to_string())
    // }
}

#[async_trait::async_trait]
impl SessionStore for FastSessionStore {
    async fn create(&self, record: &mut Record) -> Result<(), SeesionError> {
        // 로컬 캐시에 저장
        let session_json = serde_json::to_string(record)
            .map_err(|e| SeesionError::Encode(e.to_string()))?;
        
        {
            self.local_cache.put(record.id.to_string(), session_json)
                .await
                .map_err(|e| SeesionError::Backend(e.to_string()))?;
        }

        // TODO: Redis에도 저장
        // 1. Redis 클라이언트 생성
        // 2. 세션 데이터 저장 (SETEX 사용)
        // 3. TTL은 record.expiry()를 사용

        Ok(())
    }

    async fn save(&self, record: &Record) -> Result<(), SeesionError> {
        // 로컬 캐시에 저장 (기존 세션 업데이트)
        let session_json = serde_json::to_string(record)
            .map_err(|e| SeesionError::Encode(e.to_string()))?;
        
        {
            self.local_cache.put(record.id.to_string(), session_json)
                .await
                .map_err(|e| SeesionError::Backend(e.to_string()))?;
        }

        // TODO: Redis에도 저장
        // 1. Redis 클라이언트 생성
        // 2. 세션 데이터 업데이트 (SETEX 사용)
        // 3. TTL은 record.expiry()를 사용

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

        // TODO: Redis에서 조회
        // 1. Redis 클라이언트 생성
        // 2. 세션 데이터 조회 (GET 사용)
        // 3. 조회된 데이터가 있으면:
        //    - 로컬 캐시에도 저장 (TTL은 record.expiry() 사용)
        //    - Record로 변환하여 반환

        Ok(None)
    }

    async fn delete(&self, session_id: &Id) -> Result<(), SeesionError> {
        // 로컬 캐시에서 삭제
        {
            self.local_cache.delete(session_id.to_string())
                .await
                .map_err(|e| SeesionError::Backend(e.to_string()))?;
        }

        // TODO: Redis에서도 삭제
        // 1. Redis 클라이언트 생성
        // 2. 세션 데이터 삭제 (DEL 사용)

        Ok(())
    }
}