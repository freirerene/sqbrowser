use anyhow::{Context, Result};
use ratatui::style::Color;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColorConfig {
    pub border: String,
    pub text: String,
    pub number: String,
    pub selected_border: String,
    pub selected_text: String,
    pub selected_bg: String,
    pub edit_border: String,
    pub edit_text: String,
    pub edit_bg: String,
    pub header: String,
    pub status: String,
    pub error: String,
    pub help: String,
    pub help_bg: String,
    pub help_title: String,
    pub help_section_header: String,
    pub help_key: String,
    pub help_description: String,
    pub column_header: String,
    pub query_bg: String,
    pub query_text: String,
    pub query_border: String,
    pub edit_area_bg: String,
    pub detailed_view_bg: String,
    pub detailed_view_border: String,
    pub detailed_view_title: String,
    pub detailed_view_field: String,
    pub detailed_view_value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub colors: ColorConfig,
}

impl Default for ColorConfig {
    fn default() -> Self {
        Self {
            border: "#464b57ff".to_string(),
            text: "#dce0e5ff".to_string(),
            number: "#83c9d4ff".to_string(),
            selected_border: "#f1c40fff".to_string(),
            selected_text: "#000000ff".to_string(),
            selected_bg: "#00bcd4ff".to_string(),
            edit_border: "#e74c3cff".to_string(),
            edit_text: "#000000ff".to_string(),
            edit_bg: "#f1c40fff".to_string(),
            header: "#27ae60ff".to_string(),
            status: "#27ae60ff".to_string(),
            error: "#e74c3cff".to_string(),
            help: "#9b59b6ff".to_string(),
            help_bg: "#000000ff".to_string(),
            help_title: "#f39c12ff".to_string(),
            help_section_header: "#27ae60ff".to_string(),
            help_key: "#3498dbff".to_string(),
            help_description: "#ecf0f1ff".to_string(),
            column_header: "#9b59b6ff".to_string(),
            query_bg: "#2c3e50ff".to_string(),
            query_text: "#ecf0f1ff".to_string(),
            query_border: "#3498dbff".to_string(),
            edit_area_bg: "#ffffffff".to_string(),
            detailed_view_bg: "#000000ff".to_string(),
            detailed_view_border: "#f1c40fff".to_string(),
            detailed_view_title: "#f1c40fff".to_string(),
            detailed_view_field: "#3498dbff".to_string(),
            detailed_view_value: "#ecf0f1ff".to_string(),
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            colors: ColorConfig::default(),
        }
    }
}

pub struct Theme {
    pub border: Color,
    pub text: Color,
    pub number: Color,
    pub selected_border: Color,
    pub selected_text: Color,
    pub selected_bg: Color,
    pub edit_border: Color,
    pub edit_text: Color,
    pub edit_bg: Color,
    pub header: Color,
    pub status: Color,
    pub error: Color,
    pub help: Color,
    pub help_bg: Color,
    pub help_title: Color,
    pub help_section_header: Color,
    pub help_key: Color,
    pub help_description: Color,
    pub column_header: Color,
    pub query_bg: Color,
    pub query_text: Color,
    pub query_border: Color,
    pub edit_area_bg: Color,
    pub detailed_view_bg: Color,
    pub detailed_view_border: Color,
    pub detailed_view_title: Color,
    pub detailed_view_field: Color,
    pub detailed_view_value: Color,
}

impl From<&ColorConfig> for Theme {
    fn from(config: &ColorConfig) -> Self {
        Self {
            border: parse_color(&config.border).unwrap_or(Color::Cyan),
            text: parse_color(&config.text).unwrap_or(Color::White),
            number: parse_color(&config.number).unwrap_or(Color::Cyan),
            selected_border: parse_color(&config.selected_border).unwrap_or(Color::Yellow),
            selected_text: parse_color(&config.selected_text).unwrap_or(Color::Black),
            selected_bg: parse_color(&config.selected_bg).unwrap_or(Color::Cyan),
            edit_border: parse_color(&config.edit_border).unwrap_or(Color::Red),
            edit_text: parse_color(&config.edit_text).unwrap_or(Color::Black),
            edit_bg: parse_color(&config.edit_bg).unwrap_or(Color::Yellow),
            header: parse_color(&config.header).unwrap_or(Color::Green),
            status: parse_color(&config.status).unwrap_or(Color::Green),
            error: parse_color(&config.error).unwrap_or(Color::Red),
            help: parse_color(&config.help).unwrap_or(Color::Magenta),
            help_bg: parse_color(&config.help_bg).unwrap_or(Color::Black),
            help_title: parse_color(&config.help_title).unwrap_or(Color::Yellow),
            help_section_header: parse_color(&config.help_section_header).unwrap_or(Color::Green),
            help_key: parse_color(&config.help_key).unwrap_or(Color::Blue),
            help_description: parse_color(&config.help_description).unwrap_or(Color::White),
            column_header: parse_color(&config.column_header).unwrap_or(Color::Magenta),
            query_bg: parse_color(&config.query_bg).unwrap_or(Color::DarkGray),
            query_text: parse_color(&config.query_text).unwrap_or(Color::White),
            query_border: parse_color(&config.query_border).unwrap_or(Color::Blue),
            edit_area_bg: parse_color(&config.edit_area_bg).unwrap_or(Color::White),
            detailed_view_bg: parse_color(&config.detailed_view_bg).unwrap_or(Color::Black),
            detailed_view_border: parse_color(&config.detailed_view_border).unwrap_or(Color::Yellow),
            detailed_view_title: parse_color(&config.detailed_view_title).unwrap_or(Color::Yellow),
            detailed_view_field: parse_color(&config.detailed_view_field).unwrap_or(Color::Blue),
            detailed_view_value: parse_color(&config.detailed_view_value).unwrap_or(Color::White),
        }
    }
}

pub fn load_config() -> Result<Config> {
    let config_path = get_config_path()?;
    
    if config_path.exists() {
        let content = fs::read_to_string(&config_path)
            .context("Failed to read config file")?;
        let config: Config = serde_json::from_str(&content)
            .context("Failed to parse config file")?;
        Ok(config)
    } else {
        // Create default config file
        let default_config = Config::default();
        create_config_file(&config_path, &default_config)?;
        Ok(default_config)
    }
}

fn get_config_path() -> Result<PathBuf> {
    let home_dir = std::env::var("HOME")
        .context("HOME environment variable not set")?;
    let config_dir = PathBuf::from(home_dir).join(".config").join("sqbrowser");
    
    // Create config directory if it doesn't exist
    if !config_dir.exists() {
        fs::create_dir_all(&config_dir)
            .context("Failed to create config directory")?;
    }
    
    Ok(config_dir.join("config.json"))
}

fn create_config_file(path: &PathBuf, config: &Config) -> Result<()> {
    let json = serde_json::to_string_pretty(config)
        .context("Failed to serialize config")?;
    fs::write(path, json)
        .context("Failed to write config file")?;
    Ok(())
}

pub fn parse_color(hex: &str) -> Result<Color> {
    let hex = hex.trim_start_matches('#');
    
    // Handle both RGB and RGBA formats
    let (r, g, b) = match hex.len() {
        6 => {
            let r = u8::from_str_radix(&hex[0..2], 16)?;
            let g = u8::from_str_radix(&hex[2..4], 16)?;
            let b = u8::from_str_radix(&hex[4..6], 16)?;
            (r, g, b)
        }
        8 => {
            // RGBA format - ignore alpha for now
            let r = u8::from_str_radix(&hex[0..2], 16)?;
            let g = u8::from_str_radix(&hex[2..4], 16)?;
            let b = u8::from_str_radix(&hex[4..6], 16)?;
            // Alpha is at hex[6..8] but ratatui doesn't support it
            (r, g, b)
        }
        _ => return Err(anyhow::anyhow!("Invalid hex color format: {}", hex)),
    };
    
    Ok(Color::Rgb(r, g, b))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_color() {
        assert!(matches!(parse_color("#ff0000"), Ok(Color::Rgb(255, 0, 0))));
        assert!(matches!(parse_color("#00ff00ff"), Ok(Color::Rgb(0, 255, 0))));
        assert!(matches!(parse_color("464b57ff"), Ok(Color::Rgb(70, 75, 87))));
        assert!(parse_color("#invalid").is_err());
    }

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert_eq!(config.colors.border, "#464b57ff");
        assert_eq!(config.colors.text, "#dce0e5ff");
    }
}