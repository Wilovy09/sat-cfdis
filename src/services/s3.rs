use aws_sdk_s3::Client;
use aws_sdk_s3::primitives::ByteStream;

/// S3 key: `cfdis/{rfc_emisor}/{rfc_receptor}/{year}/{month:02}/{day:02}/{uuid}.xml`
#[allow(dead_code)]
fn key(
    rfc_emisor: &str,
    rfc_receptor: &str,
    year: u32,
    month: u32,
    day: u32,
    uuid: &str,
) -> String {
    format!("cfdis/{rfc_emisor}/{rfc_receptor}/{year}/{month:02}/{day:02}/{uuid}.xml")
}

/// Upload XML bytes to S3. Returns the S3 key.
#[allow(dead_code)]
pub async fn upload_xml(
    client: &Client,
    bucket: &str,
    rfc_emisor: &str,
    rfc_receptor: &str,
    year: u32,
    month: u32,
    day: u32,
    uuid: &str,
    data: Vec<u8>,
) -> Result<String, String> {
    let k = key(rfc_emisor, rfc_receptor, year, month, day, uuid);
    client
        .put_object()
        .bucket(bucket)
        .key(&k)
        .content_type("application/xml")
        .body(ByteStream::from(data))
        .send()
        .await
        .map_err(|e| e.to_string())?;
    Ok(k)
}

/// Download XML bytes from S3 if the key exists. Returns None if not found.
#[allow(dead_code)]
pub async fn get_xml(
    client: &Client,
    bucket: &str,
    rfc_emisor: &str,
    rfc_receptor: &str,
    year: u32,
    month: u32,
    day: u32,
    uuid: &str,
) -> Option<Vec<u8>> {
    let k = key(rfc_emisor, rfc_receptor, year, month, day, uuid);
    let resp = client
        .get_object()
        .bucket(bucket)
        .key(&k)
        .send()
        .await
        .ok()?;

    resp.body
        .collect()
        .await
        .map(|data| data.into_bytes().to_vec())
        .ok()
}
