//! Text processing.

use core::ops::Range;

use crate::markdown::{Fragment, FragmentContent};
use crate::markdown::TextBlock;

/// Extracts sentences from a given [`TextBlock`].
///
/// A code block is treated as a single sentence.
pub fn extract_sentences(
    text_block: &TextBlock,
) -> Vec<(String, Range<usize>)> {
    match text_block {
        TextBlock::Text(fragments) =>
            extract_sentences_from_fragments(fragments),
        TextBlock::Code { code, range, .. } =>
            vec![(code.clone(), range.clone())],
    }
}

fn extract_sentences_from_fragments(
    fragments: &Vec<Fragment>,
) -> Vec<(String, Range<usize>)> {
    let senetences: Vec<(String, Range<usize>)> = fragments
        .iter()
        .flat_map(|fragment| segment_fragment(fragment))
        .fold(Vec::with_capacity(10), |mut sentences, (token, r)| {
            match token {
                TokenType::Character(ch) => {
                    if let Some((sentence, range)) = sentences.last_mut() {
                        sentence.push(ch);
                        range.end = r.end;
                    } else {
                        sentences.push((ch.to_string(), r));
                    }
                },
                TokenType::String(s) => {
                    if let Some((sentence, range)) = sentences.last_mut() {
                        sentence.push_str(&s);
                        range.end = r.end;
                    } else {
                        sentences.push((s, r));
                    }
                },
                TokenType::SentenceBreak => {
                    sentences.push((String::with_capacity(256), r));
                },
            };
            sentences
        });
    senetences
        .into_iter()
        .filter(|(sentence, _)| !sentence.is_empty())
        .collect()
}

fn segment_fragment((content, range): &Fragment) -> Vec<Token> {
    match content {
        FragmentContent::Text(text) => segment_text(text, range),
        FragmentContent::Code(code) =>
            vec![(TokenType::String(code.clone()), range.clone())],
        FragmentContent::Url(url) =>
            vec![(TokenType::String(url.clone()), range.clone())],
    }
}

// Segments a given text at sentence breaks.
//
// A sentence breaks at a period, question mark, exclamation mark,
// semicolon, or 句点('。').
fn segment_text(text: &String, range: &Range<usize>) -> Vec<Token> {
    // labels each character
    let mut transducer = Transducer::new(range.start);
    let mut tokens: Vec<Token> = Vec::with_capacity(text.len());
    for ch in text.chars() {
        tokens.append(&mut transducer.next(ch));
    }
    tokens.append(&mut transducer.finish());
    tokens
}

struct Transducer {
    num_chars: usize,
    // `state` internally becomes `None` while it is transitioning.
    state: Option<TransducerState>,
}

#[derive(Clone, Debug)]
enum TokenType {
    // Character.
    Character(char),
    // String.
    String(String),
    // Sentence break.
    SentenceBreak,
}

type Token = (TokenType, Range<usize>);

#[derive(Clone, Debug)]
enum TransducerState {
    // Initial state.
    Initial,
    // Accepting characters in a sentence.
    Character,
    // Accepting whitespace characters.
    // Its item is the start index of the whitespace.
    Whitespace(usize),
    // Determining the end of a sentence after a period.
    // Its item is the start index of the period.
    PeriodAnd(usize),
    // Determining the end of a sentence after a period preceded by whitespace.
    // First item is the start index of the whitespace.
    // Second one is the start index of the period.
    WhitespacePeriodAnd(usize, usize),
}

impl Transducer {
    fn new(start: usize) -> Self {
        Self {
            num_chars: start,
            state: Some(TransducerState::Initial),
        }
    }

    fn next(&mut self, ch: char) -> Vec<Token> {
        let (next_state, output) = self.state.take().unwrap().next(self, ch);
        self.num_chars += 1;
        self.state.replace(next_state);
        output
    }

    fn finish(&mut self) -> Vec<Token> {
        let (next_state, output) = self.state.take().unwrap().finish(self);
        self.state.replace(next_state);
        output
    }
}

impl TransducerState {
    fn next(self, transducer: &mut Transducer, ch: char) -> (Self, Vec<Token>) {
        match self {
            Self::Initial => Self::initial_next(transducer, ch),
            Self::Character => Self::character_next(transducer, ch),
            Self::Whitespace(start) =>
                Self::whitespace_next(transducer, start, ch),
            Self::PeriodAnd(start) =>
                Self::period_and_next(transducer, start, ch),
            Self::WhitespacePeriodAnd(w_start, p_start) =>
                Self::whitespace_period_and_next(
                    transducer,
                    w_start,
                    p_start,
                    ch,
                ),
        }
    }

    fn finish(self, transducer: &mut Transducer) -> (Self, Vec<Token>) {
        match self {
            Self::Initial => Self::initial_finish(),
            Self::Character => Self::character_finish(),
            Self::Whitespace(start) =>
                Self::whitespace_finish(transducer, start),
            Self::PeriodAnd(start) => Self::period_and_finish(start),
            Self::WhitespacePeriodAnd(_, p_start) =>
                Self::whitespace_period_and_finish(p_start),
        }
    }

    fn initial_next(
        transducer: &mut Transducer,
        ch: char,
    ) -> (Self, Vec<Token>) {
        match ch {
            ch if ch.is_ascii_whitespace() => {
                // deters the output and squashes the consecutive whitespaces
                (Self::Whitespace(transducer.num_chars), Vec::new())
            },
            _ => {
                (
                    Self::Character,
                    vec![(
                        TokenType::Character(ch),
                        Range {
                            start: transducer.num_chars,
                            end: transducer.num_chars + 1,
                        }
                    )],
                )
            },
        }
    }

    fn initial_finish() -> (Self, Vec<Token>) {
        (Self::Initial, Vec::new())
    }

    fn character_next(
        transducer: &mut Transducer,
        ch: char,
    ) -> (Self, Vec<Token>) {
        match ch {
            ch if ch.is_ascii_whitespace() => {
                // deters the output and squashes consecutive whitespaces
                (Self::Whitespace(transducer.num_chars), Vec::new())
            },
            '.' => {
                // deters the output
                // and determines if this is the end of the sentence
                (Self::PeriodAnd(transducer.num_chars), Vec::new())
            },
            ch if ch.is_sentence_break() => {
                // determines this is the end of the sentence
                (
                    Self::Initial,
                    vec![
                        (
                            TokenType::Character(ch),
                            Range {
                                start: transducer.num_chars,
                                end: transducer.num_chars + 1,
                            },
                        ),
                        (
                            TokenType::SentenceBreak,
                            Range {
                                start: transducer.num_chars + 1,
                                end: transducer.num_chars + 1,
                            },
                        ),
                    ],
                )
            },
            _ => {
                (
                    Self::Character,
                    vec![(
                        TokenType::Character(ch),
                        Range {
                            start: transducer.num_chars,
                            end: transducer.num_chars + 1,
                        },
                    )],
                )
            },
        }
    }

    fn character_finish() -> (Self, Vec<Token>) {
        (Self::Initial, Vec::new())
    }

    fn whitespace_next(
        transducer: &mut Transducer,
        start: usize,
        ch: char,
    ) -> (Self, Vec<Token>) {
        match ch {
            ch if ch.is_ascii_whitespace() => {
                // deters the output and squashes consecutive whitespaces
                (Self::Whitespace(start), Vec::new())
            },
            '.' => {
                // deters the output
                // and determines if this is the end of the sentence
                (
                    Self::WhitespacePeriodAnd(start, transducer.num_chars),
                    Vec::new(),
                )
            },
            ch if ch.is_sentence_break() =>
                todo!("{}", ch),
            _ => {
                (
                    Self::Character,
                    vec![
                        (
                            TokenType::Character(' '),
                            Range {
                                start,
                                end: transducer.num_chars,
                            },
                        ),
                        (
                            TokenType::Character(ch),
                            Range {
                                start: transducer.num_chars,
                                end: transducer.num_chars + 1,
                            },
                        ),
                    ],
                )
            },
        }
    }

    fn whitespace_finish(
        transducer: &Transducer,
        start: usize,
    ) -> (Self, Vec<Token>) {
        (
            Self::Initial,
            vec![(
                TokenType::Character(' '),
                Range {
                    start,
                    end: transducer.num_chars,
                },
            )],
        )
    }

    fn period_and_next(
        transducer: &mut Transducer,
        start: usize,
        ch: char,
    ) -> (Self, Vec<Token>) {
        match ch {
            ch if ch.is_ascii_whitespace() => {
                // determines the end of the sentence
                (
                    Self::Whitespace(transducer.num_chars),
                    vec![
                        (
                            TokenType::Character('.'),
                            Range {
                                start,
                                end: start + 1,
                            },
                        ),
                        (
                            TokenType::SentenceBreak,
                            Range {
                                start: start + 1,
                                end: start + 1,
                            },
                        ),
                    ],
                )
            },
            _ => {
                // cancels the end of the sentence
                (
                    Self::Character,
                    vec![
                        (
                            TokenType::Character('.'),
                            Range {
                                start,
                                end: start + 1,
                            },
                        ),
                        (
                            TokenType::Character(ch),
                            Range {
                                start: transducer.num_chars,
                                end: transducer.num_chars + 1,
                            },
                        ),
                    ],
                )
            },
        }
    }

    fn period_and_finish(start: usize) -> (Self, Vec<Token>) {
        (
            Self::Initial,
            vec![
                (
                    TokenType::Character('.'),
                    Range {
                        start,
                        end: start + 1,
                    },
                ),
                (
                    TokenType::SentenceBreak,
                    Range {
                        start: start + 1,
                        end: start + 1,
                    },
                ),
            ],
        )
    }

    fn whitespace_period_and_next(
        transducer: &mut Transducer,
        w_start: usize,
        p_start: usize,
        ch: char,
    ) -> (Self, Vec<Token>) {
        match ch {
            ch if ch.is_ascii_whitespace() => {
                // determines the end of the sentence
                // drops the preceding whitespace
                (
                    Self::Initial,
                    vec![
                        (
                            TokenType::Character('.'),
                            Range {
                                start: p_start,
                                end: p_start + 1,
                            },
                        ),
                        (
                            TokenType::SentenceBreak,
                            Range {
                                start: p_start + 1,
                                end: p_start + 1,
                            },
                        ),
                    ],
                )
            },
            _ => {
                // cancels the end of the sentence
                // leaves the preceding whitespace
                (
                    Self::Character,
                    vec![
                        (
                            TokenType::Character(' '),
                            Range {
                                start: w_start,
                                end: p_start,
                            },
                        ),
                        (
                            TokenType::Character('.'),
                            Range {
                                start: p_start,
                                end: p_start + 1,
                            },
                        ),
                        (
                            TokenType::Character(ch),
                            Range {
                                start: transducer.num_chars,
                                end: transducer.num_chars + 1,
                            },
                        ),
                    ],
                )
            },
        }
    }

    fn whitespace_period_and_finish(p_start: usize) -> (Self, Vec<Token>) {
        // drops the preceding whitespace
        (
            Self::Initial,
            vec![
                (
                    TokenType::Character('.'),
                    Range {
                        start: p_start,
                        end: p_start + 1,
                    },
                ),
                (
                    TokenType::SentenceBreak,
                    Range {
                        start: p_start + 1,
                        end: p_start + 1,
                    },
                ),
            ],
        )
    }
}

trait CharExt {
    fn is_sentence_break(self) -> bool;
}

impl CharExt for char {
    fn is_sentence_break(self) -> bool {
        // '.' is undeterministic
        match self {
            '?' | '!' | ';' | '。' | '！' | '？' => true,
            _ => false,
        }
    }
}
