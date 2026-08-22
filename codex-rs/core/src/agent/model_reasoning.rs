use crate::config::Config;
use codex_models_manager::manager::SharedModelsManager;

/// Validates the effective model/reasoning pair after all precedence layers are resolved.
///
/// Custom models retain the existing fallback-metadata behavior because their supported
/// reasoning levels are not known locally.
pub async fn validate_model_reasoning_effort(
    config: &Config,
    models_manager: &SharedModelsManager,
) -> Result<(), String> {
    let Some(reasoning_effort) = config.model_reasoning_effort.as_ref() else {
        return Ok(());
    };
    let model = config
        .model
        .as_deref()
        .ok_or_else(|| "could not resolve the model for reasoning effort validation".to_string())?;
    let model_info = models_manager
        .get_model_info(model, &config.to_models_manager_config())
        .await;
    if model_info.used_fallback_model_metadata {
        return Ok(());
    }
    if model_info
        .supported_reasoning_levels
        .iter()
        .any(|preset| &preset.effort == reasoning_effort)
    {
        return Ok(());
    }

    let supported = model_info
        .supported_reasoning_levels
        .iter()
        .map(|preset| preset.effort.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    Err(format!(
        "Reasoning effort `{reasoning_effort}` is not supported for model `{model}`. Supported reasoning efforts: {supported}"
    ))
}
