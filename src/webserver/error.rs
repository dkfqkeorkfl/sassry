use cassry::*;

#[macro_export]
macro_rules! errcodes {
    (
        $($num:expr => $name:ident $(($param:ty))?),* $(,)?
    ) => {
        pub mod used {
            crate::paste::paste! {
                $(
                    pub const [<ERR_ $num>]: u64 = $num;
                )*
            }
        }

        $(
            #[derive(Debug)]
            pub struct $name $((pub $param))?;

            impl $name {
                pub const CODE: u64 = $num;
            }
            
            impl crate::webserver::ErrorCode for $name {
                fn code(&self) -> u64 {
                    Self::CODE
                }

                fn name(&self) -> &'static str {
                    stringify!($name)
                }

                fn desc(&self) -> &'static str {
                    concat!(stringify!($name), " @ ", file!(), ":", line!())
                }
            }
        )*
    };
}

pub trait ErrorCode {
    fn code(&self) -> u64;
    fn name(&self) -> &'static str;
    fn desc(&self) -> &'static str;
}

pub enum RespError {
    Error(Box<dyn ErrorCode>),
    Internal(anyhow::Error),
}

impl From<tower_sessions::session::Error> for RespError {
    fn from(err: tower_sessions::session::Error) -> Self {
        RespError::Internal(anyhow::Error::from(err))
    }
}

impl From<serde_json::Error> for RespError {
    fn from(err: serde_json::Error) -> Self {
        RespError::Internal(anyhow::Error::from(err))
    }
}

impl From<sqlx::Error> for RespError {
    fn from(err: sqlx::Error) -> Self {
        RespError::Internal(anyhow::Error::from(err))
    }
}

pub mod errcode {
    errcodes! {
        1000 => MissingCookieHeader,
        // 1000 => MissingCookieHeader1,
        1001 => MissingJwtToken,
        1002 => InvalidJwt((String,String)),
        1003 => Internal(String),
    }
}
