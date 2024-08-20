use std::ops::AddAssign;

use serde::Serialize;

/// Estructura que contiene la cantidad de preguntas en las que aparece un Tag, y la cantidad de palabras de todas las preguntas en las que aparece ese Tag.
#[derive(Debug, Serialize, Clone, Copy)]
pub struct Tag {
    pub questions: usize,
    pub words: usize,
}

impl Tag {
    /// Construye un nuevo tag con la cantidad de preguntas y palabras indicadas.
    pub fn new(questions: usize, words: usize) -> Tag {
        Tag { questions, words }
    }

    /// Suma otro Tag a sí mismo, in-place.
    pub fn add(&mut self, site: &Tag) {
        self.questions += site.questions;
        self.words += site.words;
    }
}

impl AddAssign for Tag {
    /// Implementa el método += para el Tag.
    fn add_assign(&mut self, other: Self) {
        *self = Self {
            questions: self.questions + other.questions,
            words: self.words + other.words,
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_tag_correctly() {
        let tag = Tag::new(2, 10);
        assert_eq!(tag.questions, 2);
        assert_eq!(tag.words, 10);
    }

    #[test]
    fn add_tags_in_place() {
        let mut tag1 = Tag::new(2, 10);
        let tag2 = Tag::new(1, 5);
        tag1.add(&tag2);

        // Se sumaron las words y questions al tag 1
        assert_eq!(tag1.questions, 3);
        assert_eq!(tag1.words, 15);

        // El tag 2 no se modifico
        assert_eq!(tag2.questions, 1);
        assert_eq!(tag2.words, 5);
    }

    #[test]
    fn add_assign_method_for_tags() {
        let mut tag1 = Tag::new(2, 10);
        let tag2 = Tag::new(1, 5);
        tag1 += tag2;

        // Se sumaron las words y questions al tag 1
        assert_eq!(tag1.questions, 3);
        assert_eq!(tag1.words, 15);

        // El tag 2 no se modifico
        assert_eq!(tag2.questions, 1);
        assert_eq!(tag2.words, 5);
    }
}
