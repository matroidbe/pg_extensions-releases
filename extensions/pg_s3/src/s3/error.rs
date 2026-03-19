//! S3 error types with HTTP status codes and XML rendering

use std::fmt;

/// S3-compatible error types
pub enum S3Error {
    NoSuchBucket(String),
    NoSuchKey(String),
    BucketAlreadyOwnedByYou(String),
    BucketNotEmpty(String),
    InvalidBucketName(String),
    EntityTooLarge,
    InternalError(String),
    MethodNotAllowed,
}

impl S3Error {
    pub fn status_code(&self) -> u16 {
        match self {
            S3Error::NoSuchBucket(_) => 404,
            S3Error::NoSuchKey(_) => 404,
            S3Error::BucketAlreadyOwnedByYou(_) => 409,
            S3Error::BucketNotEmpty(_) => 409,
            S3Error::InvalidBucketName(_) => 400,
            S3Error::EntityTooLarge => 400,
            S3Error::InternalError(_) => 500,
            S3Error::MethodNotAllowed => 405,
        }
    }

    pub fn code(&self) -> &str {
        match self {
            S3Error::NoSuchBucket(_) => "NoSuchBucket",
            S3Error::NoSuchKey(_) => "NoSuchKey",
            S3Error::BucketAlreadyOwnedByYou(_) => "BucketAlreadyOwnedByYou",
            S3Error::BucketNotEmpty(_) => "BucketNotEmpty",
            S3Error::InvalidBucketName(_) => "InvalidBucketName",
            S3Error::EntityTooLarge => "EntityTooLarge",
            S3Error::InternalError(_) => "InternalError",
            S3Error::MethodNotAllowed => "MethodNotAllowed",
        }
    }

    pub fn resource(&self) -> &str {
        match self {
            S3Error::NoSuchBucket(r)
            | S3Error::NoSuchKey(r)
            | S3Error::BucketAlreadyOwnedByYou(r)
            | S3Error::BucketNotEmpty(r)
            | S3Error::InvalidBucketName(r)
            | S3Error::InternalError(r) => r,
            S3Error::EntityTooLarge => "/",
            S3Error::MethodNotAllowed => "/",
        }
    }

    pub fn to_xml(&self) -> String {
        crate::s3::xml::error_xml(self.code(), &self.to_string(), self.resource())
    }

    pub fn to_response(&self) -> super::http::HttpResponse {
        super::http::HttpResponse::xml(self.status_code(), &self.to_xml())
    }
}

impl fmt::Display for S3Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            S3Error::NoSuchBucket(b) => write!(f, "The specified bucket does not exist: {}", b),
            S3Error::NoSuchKey(k) => write!(f, "The specified key does not exist: {}", k),
            S3Error::BucketAlreadyOwnedByYou(b) => {
                write!(f, "Bucket already exists: {}", b)
            }
            S3Error::BucketNotEmpty(b) => write!(f, "The bucket is not empty: {}", b),
            S3Error::InvalidBucketName(b) => write!(f, "Invalid bucket name: {}", b),
            S3Error::EntityTooLarge => write!(f, "Entity too large"),
            S3Error::InternalError(e) => write!(f, "Internal error: {}", e),
            S3Error::MethodNotAllowed => write!(f, "Method not allowed"),
        }
    }
}
