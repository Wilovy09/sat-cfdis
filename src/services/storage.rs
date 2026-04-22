/// Abstraction over XML storage.
/// - `debug_assertions` ON  → local filesystem (`./invoices/{rfc_emisor}/{rfc_receptor}/{year}/{month:02}/{day:02}/{uuid}.xml`)
/// - `debug_assertions` OFF → AWS S3 with path `cfdis/{rfc_emisor}/{rfc_receptor}/{year}/{month:02}/{day:02}/{uuid}.xml`

#[cfg(debug_assertions)]
const LOCAL_BASE: &str = "invoices";

// ---------------------------------------------------------------------------
// Upload
// ---------------------------------------------------------------------------

pub async fn upload(
    #[allow(unused_variables)] s3: &aws_sdk_s3::Client,
    #[allow(unused_variables)] bucket: &str,
    rfc_emisor: &str,
    rfc_receptor: &str,
    year: u32,
    month: u32,
    day: u32,
    uuid: &str,
    data: Vec<u8>,
) -> Result<(), String> {
    #[cfg(debug_assertions)]
    {
        let dir = format!("{LOCAL_BASE}/{rfc_emisor}/{rfc_receptor}/{year}/{month:02}/{day:02}");
        let path = format!("{dir}/{uuid}.xml");
        tokio::fs::create_dir_all(&dir)
            .await
            .map_err(|e| e.to_string())?;
        tokio::fs::write(&path, &data)
            .await
            .map_err(|e| e.to_string())?;
        return Ok(());
    }

    #[cfg(not(debug_assertions))]
    {
        super::s3::upload_xml(
            s3,
            bucket,
            rfc_emisor,
            rfc_receptor,
            year,
            month,
            day,
            uuid,
            data,
        )
        .await?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Get
// ---------------------------------------------------------------------------

pub async fn get(
    #[allow(unused_variables)] s3: &aws_sdk_s3::Client,
    #[allow(unused_variables)] bucket: &str,
    rfc_emisor: &str,
    rfc_receptor: &str,
    year: u32,
    month: u32,
    day: u32,
    uuid: &str,
) -> Option<Vec<u8>> {
    #[cfg(debug_assertions)]
    {
        let path = format!(
            "{LOCAL_BASE}/{rfc_emisor}/{rfc_receptor}/{year}/{month:02}/{day:02}/{uuid}.xml"
        );
        tokio::fs::read(&path).await.ok()
    }

    #[cfg(not(debug_assertions))]
    {
        super::s3::get_xml(s3, bucket, rfc_emisor, rfc_receptor, year, month, day, uuid).await
    }
}
