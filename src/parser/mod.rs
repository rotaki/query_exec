mod translator;

pub use translator::{Query, Translator, TranslatorError};

pub mod prelude {
    pub use super::{Query, Translator, TranslatorError};
}
