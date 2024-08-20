use serde::Deserialize;

/// Estructura que contiene los texts y tags de cada línea de los JSON. Se usa para deserializarlos en una estructura y manejar los contenidos del archivo.
#[derive(Deserialize)]
pub struct Line {
    pub texts: Vec<String>,
    pub tags: Vec<String>,
}
