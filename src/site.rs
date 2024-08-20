use std::collections::HashMap;

use serde::Serialize;

use crate::tag::Tag;

/// Estructura que contiene la cantidad de preguntas y palabras de un Site, los tags que aparecen en él y los 10 con mayor ratio palabras/preguntas.
#[derive(Debug, Serialize, Clone)]
pub struct Site {
    pub questions: usize,
    pub words: usize,
    pub tags: HashMap<String, Tag>,
    pub chatty_tags: Vec<String>,
}

impl Site {
    /// Construye un nuevo Site con los parámetros indicados.
    pub fn new(
        questions: usize,
        words: usize,
        tags: HashMap<String, Tag>,
        chatty_tags: Vec<String>,
    ) -> Site {
        Site {
            questions,
            words,
            tags,
            chatty_tags,
        }
    }

    /// Suma un Site a sí mismo in-place, sin duplicar Tags, sino que se suman los contenidos de aquellos que estén repetidos.
    pub fn add(&mut self, site: &Site) {
        self.questions += site.questions;
        self.words += site.words;
        site.tags.iter().for_each(|(tag_name, tag)| {
            self.tags
                .entry(tag_name.to_string())
                .and_modify(|t| *t += *tag)
                .or_insert(*tag);
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_site_correctly() {
        let site = Site::new(
            2,
            10,
            HashMap::from([("tag_1".to_string(), Tag::new(2, 10))]),
            vec!["chatty_1".to_string()],
        );

        assert_eq!(site.questions, 2);
        assert_eq!(site.words, 10);
        assert!(site.tags.contains_key("tag_1"));
        assert_eq!(site.tags.get("tag_1").unwrap().questions, 2);
        assert_eq!(site.tags.get("tag_1").unwrap().words, 10);
        assert_eq!(site.chatty_tags[0], "chatty_1".to_string());
    }

    #[test]
    fn add_sites_in_place() {
        let mut site1 = Site::new(
            2,
            10,
            HashMap::from([("tag_1".to_string(), Tag::new(2, 10))]),
            vec!["chatty_1".to_string()],
        );
        let site2 = Site::new(
            1,
            5,
            HashMap::from([("tag_2".to_string(), Tag::new(1, 5))]),
            vec!["chatty_2".to_string()],
        );
        site1.add(&site2);

        // Se sumaron las words y questions al site 1
        assert_eq!(site1.questions, 3);
        assert_eq!(site1.words, 15);
        assert!(site1.tags.contains_key("tag_1"));
        assert!(site1.tags.contains_key("tag_2"));
        assert_eq!(site1.tags.get("tag_1").unwrap().questions, 2);
        assert_eq!(site1.tags.get("tag_1").unwrap().words, 10);
        assert_eq!(site1.tags.get("tag_2").unwrap().questions, 1);
        assert_eq!(site1.tags.get("tag_2").unwrap().words, 5);
        assert_eq!(site1.chatty_tags[0], "chatty_1".to_string());

        // El site 2 no se modifico
        assert_eq!(site2.questions, 1);
        assert_eq!(site2.words, 5);
        assert!(site2.tags.contains_key("tag_2"));
        assert_eq!(site2.tags.get("tag_2").unwrap().questions, 1);
        assert_eq!(site2.tags.get("tag_2").unwrap().words, 5);
    }
}
