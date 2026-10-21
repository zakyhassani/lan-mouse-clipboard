//! Windows CF_DIB/PNG clipboard image conversion.

use std::io::Cursor;
use std::sync::OnceLock;

use image::{ImageFormat, ImageReader};
use windows::Win32::Foundation::HGLOBAL;
use windows::Win32::Graphics::Gdi::{
    BI_BITFIELDS, BI_JPEG, BI_PNG, BI_RGB, BITMAPINFOHEADER, BITMAPV5HEADER, LCS_GM_IMAGES,
};
use windows::Win32::System::DataExchange::{GetClipboardData, RegisterClipboardFormatW};
use windows::Win32::System::Memory::{GlobalLock, GlobalSize, GlobalUnlock};
use windows::Win32::System::Ole::{CF_DIB, CF_DIBV5};
use windows::core::w;

const LCS_SRGB: u32 = 0x7352_4742;

/// Registered PNG clipboard format. The clipboard format id is stable for a
/// session, so cache it.
pub fn png_format() -> u32 {
    static FORMAT: OnceLock<u32> = OnceLock::new();
    *FORMAT.get_or_init(|| unsafe { RegisterClipboardFormatW(w!("PNG")) })
}

/// Read an image while the clipboard is open. PNG is preferred; otherwise
/// CF_DIBV5/CF_DIB is converted to PNG.
pub fn read_image(max_item_size: usize) -> Option<(String, Vec<u8>)> {
    if let Some(data) = clipboard_bytes(png_format(), max_item_size) {
        if data.starts_with(b"\x89PNG\r\n\x1a\n") {
            return Some(("image/png".into(), data));
        }
    }
    let dib_limit = max_item_size
        .saturating_mul(8)
        .clamp(64 * 1024 * 1024, 512 * 1024 * 1024);
    for format in [CF_DIBV5.0 as u32, CF_DIB.0 as u32] {
        if let Some(data) = clipboard_bytes(format, dib_limit) {
            if let Some(png) = dib_to_png(&data, max_item_size) {
                return Some(("image/png".into(), png));
            }
        }
    }
    None
}

fn clipboard_bytes(format: u32, max: usize) -> Option<Vec<u8>> {
    let handle = unsafe { GetClipboardData(format) }.ok()?;
    let memory = HGLOBAL(handle.0);
    let size = unsafe { GlobalSize(memory) };
    if size == 0 || size > max {
        return None;
    }
    let locked = unsafe { GlobalLock(memory) };
    if locked.is_null() {
        return None;
    }
    let data = unsafe { std::slice::from_raw_parts(locked as *const u8, size) }.to_vec();
    unsafe {
        let _ = GlobalUnlock(memory);
    }
    Some(data)
}

/// Build clipboard formats for an image item.
///
/// Returns `(clipboard format, bytes)` entries for CF_DIBV5, CF_DIB and the
/// registered PNG format. Formats larger than `max_item_size` are omitted; if
/// none fit, the image is rejected.
pub fn build_image_formats(
    mime: &str,
    data: &[u8],
    max_item_size: usize,
) -> Result<Vec<(u32, Vec<u8>)>, String> {
    // Parse dimensions before decoding so a small compressed image cannot
    // force a large RGBA allocation that is immediately rejected.
    let (width, height) = image_dimensions(data)?;
    if width > 32768 || height > 32768 {
        return Err("image dimensions exceed the Windows limits".into());
    }
    let pixel_count = width as u64 * height as u64;
    if pixel_count > 50_000_000 {
        return Err("image dimensions exceed the Windows limits".into());
    }
    let pixel_bytes = pixel_count
        .checked_mul(4)
        .ok_or_else(|| "image size overflow".to_string())?;
    let dibv5_len = std::mem::size_of::<BITMAPV5HEADER>() as u64 + pixel_bytes;
    let dib_len = std::mem::size_of::<BITMAPINFOHEADER>() as u64 + pixel_bytes;
    let max = max_item_size as u64;
    let need_pixels = dibv5_len <= max || dib_len <= max;
    let original_png = mime
        .eq_ignore_ascii_case("image/png")
        .then(|| data.to_vec());

    let (rgba, pixels) = if need_pixels {
        let decoded = decode(data, None, pixel_bytes.saturating_add(64 * 1024 * 1024))?;
        let rgba = decoded.to_rgba8();
        let (width, height) = rgba.dimensions();
        if width > 32768 || height > 32768 || width as u64 * height as u64 > 50_000_000 {
            return Err("image dimensions exceed the Windows limits".into());
        }
        let stride = width as usize * 4;
        let mut pixels = Vec::with_capacity(stride * height as usize);
        for y in (0..height).rev() {
            for x in 0..width {
                let pixel = rgba.get_pixel(x, y);
                pixels.extend_from_slice(&[pixel[2], pixel[1], pixel[0], pixel[3]]);
            }
        }
        (Some(rgba), Some((width, height, pixels)))
    } else {
        (None, None)
    };

    let mut formats = Vec::new();
    if let Some((width, height, pixels)) = &pixels {
        let stride = *width as usize * 4;
        let v5 = BITMAPV5HEADER {
            bV5Size: std::mem::size_of::<BITMAPV5HEADER>() as u32,
            bV5Width: *width as i32,
            bV5Height: *height as i32,
            bV5Planes: 1,
            bV5BitCount: 32,
            bV5Compression: BI_BITFIELDS,
            bV5SizeImage: (stride * *height as usize) as u32,
            bV5RedMask: 0x00ff_0000,
            bV5GreenMask: 0x0000_ff00,
            bV5BlueMask: 0x0000_00ff,
            bV5AlphaMask: 0xff00_0000,
            bV5CSType: LCS_SRGB,
            bV5Intent: LCS_GM_IMAGES as u32,
            ..Default::default()
        };
        let mut dibv5 = header_bytes(&v5);
        dibv5.extend_from_slice(pixels);
        if dibv5.len() <= max_item_size {
            formats.push((CF_DIBV5.0 as u32, dibv5));
        }

        let info = BITMAPINFOHEADER {
            biSize: std::mem::size_of::<BITMAPINFOHEADER>() as u32,
            biWidth: *width as i32,
            biHeight: *height as i32,
            biPlanes: 1,
            biBitCount: 32,
            biCompression: BI_RGB.0,
            biSizeImage: (stride * *height as usize) as u32,
            ..Default::default()
        };
        let mut dib = header_bytes(&info);
        dib.extend_from_slice(pixels);
        if dib.len() <= max_item_size {
            formats.push((CF_DIB.0 as u32, dib));
        }
    }

    let png = match original_png {
        Some(png) => png,
        None => {
            let rgba = match &rgba {
                Some(rgba) => rgba,
                None => {
                    &decode(data, None, pixel_bytes.saturating_add(64 * 1024 * 1024))?.to_rgba8()
                }
            };
            encode_png(rgba)?
        }
    };
    if png.len() <= max_item_size {
        formats.push((png_format(), png));
    }
    if formats.is_empty() {
        return Err("image does not fit the clipboard item cap".into());
    }
    Ok(formats)
}

fn image_dimensions(data: &[u8]) -> Result<(u32, u32), String> {
    let mut reader = ImageReader::new(Cursor::new(data))
        .with_guessed_format()
        .map_err(|e| e.to_string())?;
    let mut limits = image::Limits::default();
    limits.max_image_width = Some(32768);
    limits.max_image_height = Some(32768);
    limits.max_alloc = Some(16 * 1024 * 1024);
    reader.limits(limits);
    reader.into_dimensions().map_err(|e| e.to_string())
}

fn header_bytes<T>(value: &T) -> Vec<u8> {
    unsafe {
        std::slice::from_raw_parts(value as *const T as *const u8, std::mem::size_of::<T>())
            .to_vec()
    }
}

fn encode_png(image: &image::RgbaImage) -> Result<Vec<u8>, String> {
    let mut out = Vec::new();
    image::DynamicImage::ImageRgba8(image.clone())
        .write_to(&mut Cursor::new(&mut out), ImageFormat::Png)
        .map_err(|e| e.to_string())?;
    Ok(out)
}

fn decode(
    data: &[u8],
    format: Option<ImageFormat>,
    max_alloc: u64,
) -> Result<image::DynamicImage, String> {
    let max_alloc = max_alloc.clamp(16 * 1024 * 1024, 512 * 1024 * 1024);
    let mut reader = match format {
        Some(format) => ImageReader::with_format(Cursor::new(data), format),
        None => ImageReader::new(Cursor::new(data))
            .with_guessed_format()
            .map_err(|e| e.to_string())?,
    };
    let mut limits = image::Limits::default();
    limits.max_image_width = Some(32768);
    limits.max_image_height = Some(32768);
    limits.max_alloc = Some(max_alloc);
    reader.limits(limits);
    reader.decode().map_err(|e| e.to_string())
}

fn dib_to_png(bytes: &[u8], max_item_size: usize) -> Option<Vec<u8>> {
    if bytes.len() < 40 {
        return None;
    }
    let header_size = u32::from_le_bytes(bytes[0..4].try_into().ok()?) as usize;
    if !(40..=bytes.len()).contains(&header_size) {
        return None;
    }
    let compression = u32::from_le_bytes(bytes[16..20].try_into().ok()?);
    if compression == BI_PNG.0 || compression == BI_JPEG.0 {
        let image = decode(&bytes[header_size..], None, 512 * 1024 * 1024).ok()?;
        let rgba = image.to_rgba8();
        return encode_png(&rgba)
            .ok()
            .filter(|png| png.len() <= max_item_size);
    }
    if header_size < 40 {
        return None;
    }
    let bit_count = u16::from_le_bytes(bytes[14..16].try_into().ok()?);
    let clr_used = u32::from_le_bytes(bytes[32..36].try_into().ok()?);
    let palette_entries = if bit_count <= 8 {
        if clr_used == 0 {
            1usize.checked_shl(bit_count as u32)?
        } else {
            clr_used as usize
        }
    } else {
        0
    };
    let masks = if header_size == 40 && compression == BI_BITFIELDS.0 {
        12
    } else if header_size == 40 && compression == 6 {
        // BI_ALPHABITFIELDS carries an additional alpha mask.
        16
    } else {
        0
    };
    let off_bits = 14usize
        .checked_add(header_size)?
        .checked_add(masks)?
        .checked_add(palette_entries.checked_mul(4)?)?;
    let mut bmp = Vec::with_capacity(14 + bytes.len());
    bmp.extend_from_slice(b"BM");
    bmp.extend_from_slice(&((14 + bytes.len()) as u32).to_le_bytes());
    bmp.extend_from_slice(&0u16.to_le_bytes());
    bmp.extend_from_slice(&0u16.to_le_bytes());
    bmp.extend_from_slice(&(off_bits as u32).to_le_bytes());
    bmp.extend_from_slice(bytes);
    decode(&bmp, Some(ImageFormat::Bmp), 512 * 1024 * 1024)
        .ok()
        .and_then(|image| encode_png(&image.to_rgba8()).ok())
        .filter(|png| png.len() <= max_item_size)
}
