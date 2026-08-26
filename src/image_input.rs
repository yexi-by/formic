use std::io::{self, Cursor, Read};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use base64::Engine as _;
use tokio_util::sync::CancellationToken;

use crate::tools::ReadRoot;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImageMediaType {
    Jpeg,
    Png,
    Gif,
    Webp,
}

impl ImageMediaType {
    pub const fn mime(self) -> &'static str {
        match self {
            Self::Jpeg => "image/jpeg",
            Self::Png => "image/png",
            Self::Gif => "image/gif",
            Self::Webp => "image/webp",
        }
    }

    pub const fn extension(self) -> &'static str {
        match self {
            Self::Jpeg => "jpg",
            Self::Png => "png",
            Self::Gif => "gif",
            Self::Webp => "webp",
        }
    }

    fn image_format(self) -> image::ImageFormat {
        match self {
            Self::Jpeg => image::ImageFormat::Jpeg,
            Self::Png => image::ImageFormat::Png,
            Self::Gif => image::ImageFormat::Gif,
            Self::Webp => image::ImageFormat::WebP,
        }
    }

    fn from_image_format(format: image::ImageFormat) -> Option<Self> {
        Some(match format {
            image::ImageFormat::Jpeg => Self::Jpeg,
            image::ImageFormat::Png => Self::Png,
            image::ImageFormat::Gif => Self::Gif,
            image::ImageFormat::WebP => Self::Webp,
            _ => return None,
        })
    }

    pub fn from_path(path: &Path) -> Option<Self> {
        let extension = path.extension()?.to_str()?.to_ascii_lowercase();
        match extension.as_str() {
            "jpg" | "jpeg" => Some(Self::Jpeg),
            "png" => Some(Self::Png),
            "gif" => Some(Self::Gif),
            "webp" => Some(Self::Webp),
            _ => None,
        }
    }

    fn from_mime(mime: &str) -> Option<Self> {
        match mime {
            "image/jpeg" => Some(Self::Jpeg),
            "image/png" => Some(Self::Png),
            "image/gif" => Some(Self::Gif),
            "image/webp" => Some(Self::Webp),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImageData {
    bytes: Arc<Vec<u8>>,
    media_type: ImageMediaType,
    width: u32,
    height: u32,
}

impl ImageData {
    fn inspect(bytes: Vec<u8>, expected: ImageMediaType) -> Result<Self, ImageError> {
        let detected = image::guess_format(&bytes)
            .ok()
            .and_then(ImageMediaType::from_image_format)
            .ok_or(ImageError::InvalidImage)?;
        if detected != expected {
            return Err(ImageError::MediaMismatch {
                declared: expected.mime(),
                detected: detected.mime(),
            });
        }
        let (width, height) =
            image::ImageReader::with_format(Cursor::new(bytes.as_slice()), expected.image_format())
                .into_dimensions()
                .map_err(|_| ImageError::InvalidImage)?;
        if width == 0 || height == 0 {
            return Err(ImageError::InvalidImage);
        }
        Ok(Self {
            bytes: Arc::new(bytes),
            media_type: expected,
            width,
            height,
        })
    }

    pub fn bytes(&self) -> &[u8] {
        self.bytes.as_slice()
    }

    pub fn byte_len(&self) -> usize {
        self.bytes.len()
    }

    pub const fn media_type(&self) -> ImageMediaType {
        self.media_type
    }

    pub const fn width(&self) -> u32 {
        self.width
    }

    pub const fn height(&self) -> u32 {
        self.height
    }

    pub fn base64(&self) -> Result<String, ImageError> {
        encode_base64(self.bytes())
    }

    pub fn data_url(&self) -> Result<String, ImageError> {
        let prefix = format!("data:{};base64,", self.media_type.mime());
        let encoded_len = encoded_length(self.bytes.len())?;
        let total = prefix
            .len()
            .checked_add(encoded_len)
            .ok_or(ImageError::Arithmetic)?;
        let mut output = String::new();
        output
            .try_reserve_exact(total)
            .map_err(|_| ImageError::Allocation)?;
        output.push_str(&prefix);
        base64::engine::general_purpose::STANDARD.encode_string(self.bytes(), &mut output);
        Ok(output)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ImageSource {
    Input(PathBuf),
    Archived(PathBuf),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImageInput {
    pub data: ImageData,
    pub source: ImageSource,
    pub estimated_tokens: u64,
}

impl ImageInput {
    pub fn audit_value(&self) -> serde_json::Value {
        let (kind, path) = match &self.source {
            ImageSource::Input(path) => ("input", path),
            ImageSource::Archived(path) => ("run_media", path),
        };
        serde_json::json!({
            "type": "image",
            "source": {
                "kind": kind,
                "path": crate::prompt::slash_path(path),
            },
            "mime_type": self.data.media_type().mime(),
            "bytes": self.data.byte_len(),
            "width": self.data.width(),
            "height": self.data.height(),
            "estimated_tokens": self.estimated_tokens,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ToolImage {
    Input { path: PathBuf, data: ImageData },
    Mcp(ImageData),
}

impl ToolImage {
    pub fn data(&self) -> &ImageData {
        match self {
            Self::Input { data, .. } | Self::Mcp(data) => data,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ImageError {
    #[error("只支持 JPEG、PNG、GIF 或 WebP 图片")]
    UnsupportedType,
    #[error("图片声明为 {declared}，但文件内容是 {detected}")]
    MediaMismatch {
        declared: &'static str,
        detected: &'static str,
    },
    #[error("图片内容损坏或无法读取尺寸")]
    InvalidImage,
    #[error("图片 base64 无效")]
    InvalidBase64,
    #[error("读取图片失败：{0}")]
    Io(#[from] io::Error),
    #[error("图片内存分配失败")]
    Allocation,
    #[error("图片容量计算溢出")]
    Arithmetic,
    #[error("图片读取已取消")]
    Cancelled,
}

pub fn read_input_image(
    root: &ReadRoot,
    path: &Path,
    cancel: &CancellationToken,
) -> Result<ImageData, ImageError> {
    let expected = ImageMediaType::from_path(path).ok_or(ImageError::UnsupportedType)?;
    let bytes = read_frozen_bytes(root, path, cancel)?;
    ImageData::inspect(bytes, expected)
}

pub fn decode_mcp_image(data: &str, mime: &str) -> Result<ImageData, ImageError> {
    let expected = ImageMediaType::from_mime(mime).ok_or(ImageError::UnsupportedType)?;
    let capacity = data
        .len()
        .checked_add(3)
        .and_then(|value| value.checked_div(4))
        .and_then(|value| value.checked_mul(3))
        .ok_or(ImageError::Arithmetic)?;
    let mut decoded = reserved_vec(capacity)?;
    base64::engine::general_purpose::STANDARD
        .decode_vec(data, &mut decoded)
        .map_err(|_| ImageError::InvalidBase64)?;
    ImageData::inspect(decoded, expected)
}

fn read_frozen_bytes(
    root: &ReadRoot,
    path: &Path,
    cancel: &CancellationToken,
) -> Result<Vec<u8>, ImageError> {
    if cancel.is_cancelled() {
        return Err(ImageError::Cancelled);
    }
    let mut file = root.open_file(path)?;
    let initial = usize::try_from(file.metadata()?.len()).map_err(|_| ImageError::Arithmetic)?;
    let mut bytes = reserved_vec(initial)?;
    let mut buffer = [0u8; 64 * 1024];
    loop {
        if cancel.is_cancelled() {
            return Err(ImageError::Cancelled);
        }
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        bytes
            .try_reserve(read)
            .map_err(|_| ImageError::Allocation)?;
        bytes.extend_from_slice(&buffer[..read]);
    }
    Ok(bytes)
}

fn encode_base64(bytes: &[u8]) -> Result<String, ImageError> {
    let capacity = encoded_length(bytes.len())?;
    let mut output = String::new();
    output
        .try_reserve_exact(capacity)
        .map_err(|_| ImageError::Allocation)?;
    base64::engine::general_purpose::STANDARD.encode_string(bytes, &mut output);
    Ok(output)
}

fn encoded_length(length: usize) -> Result<usize, ImageError> {
    length
        .checked_add(2)
        .and_then(|value| value.checked_div(3))
        .and_then(|value| value.checked_mul(4))
        .ok_or(ImageError::Arithmetic)
}

fn reserved_vec(capacity: usize) -> Result<Vec<u8>, ImageError> {
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(capacity)
        .map_err(|_| ImageError::Allocation)?;
    Ok(bytes)
}

#[cfg(test)]
pub(crate) fn test_image_input(source: ImageSource) -> ImageInput {
    use image::{DynamicImage, ImageFormat};

    let mut output = Cursor::new(Vec::new());
    DynamicImage::new_rgb8(3, 2)
        .write_to(&mut output, ImageFormat::Png)
        .unwrap();
    ImageInput {
        data: ImageData::inspect(output.into_inner(), ImageMediaType::Png).unwrap(),
        source,
        estimated_tokens: 255,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use image::{DynamicImage, ImageFormat};

    fn encoded(format: ImageFormat) -> Vec<u8> {
        let image = DynamicImage::new_rgb8(3, 2);
        let mut output = Cursor::new(Vec::new());
        image.write_to(&mut output, format).unwrap();
        output.into_inner()
    }

    #[test]
    fn reads_each_supported_format_without_changing_original_bytes() {
        for (format, extension, media_type) in [
            (ImageFormat::Jpeg, "jpg", ImageMediaType::Jpeg),
            (ImageFormat::Png, "png", ImageMediaType::Png),
            (ImageFormat::Gif, "gif", ImageMediaType::Gif),
            (ImageFormat::WebP, "webp", ImageMediaType::Webp),
        ] {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join(format!("sample.{extension}"));
            let bytes = encoded(format);
            std::fs::write(&path, &bytes).unwrap();
            let root = ReadRoot::open(directory.path().to_path_buf()).unwrap();
            let image = read_input_image(
                &root,
                Path::new(path.file_name().unwrap()),
                &CancellationToken::new(),
            )
            .unwrap();
            assert_eq!(image.bytes(), bytes);
            assert_eq!(image.media_type(), media_type);
            assert_eq!((image.width(), image.height()), (3, 2));
        }
    }

    #[test]
    fn rejects_corrupt_mismatched_and_invalid_base64_images() {
        assert!(matches!(
            ImageData::inspect(b"not an image".to_vec(), ImageMediaType::Png),
            Err(ImageError::InvalidImage)
        ));
        assert!(matches!(
            ImageData::inspect(encoded(ImageFormat::Png), ImageMediaType::Jpeg),
            Err(ImageError::MediaMismatch { .. })
        ));
        assert!(matches!(
            decode_mcp_image("%%%", "image/png"),
            Err(ImageError::InvalidBase64)
        ));
        assert!(matches!(
            decode_mcp_image("AA==", "image/svg+xml"),
            Err(ImageError::UnsupportedType)
        ));
    }

    #[test]
    fn cancellation_and_capacity_failures_are_explicit() {
        let directory = tempfile::tempdir().unwrap();
        std::fs::write(
            directory.path().join("sample.png"),
            encoded(ImageFormat::Png),
        )
        .unwrap();
        let root = ReadRoot::open(directory.path().to_path_buf()).unwrap();
        let cancel = CancellationToken::new();
        cancel.cancel();
        assert!(matches!(
            read_input_image(&root, Path::new("sample.png"), &cancel),
            Err(ImageError::Cancelled)
        ));
        assert!(matches!(
            encoded_length(usize::MAX),
            Err(ImageError::Arithmetic)
        ));
        assert!(matches!(
            reserved_vec(usize::MAX),
            Err(ImageError::Allocation)
        ));
    }
}
