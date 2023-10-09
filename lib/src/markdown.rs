//! Processes Markdown text.

use core::ops::Range;
use pulldown_cmark::{CodeBlockKind, Event, Options, Parser, Tag};

use crate::error::Error;

/// Text block in a Markdown text.
#[derive(Clone, Debug, PartialEq)]
pub enum TextBlock {
    /// Text block.
    Text(Vec<Fragment>),
    /// Code block.
    Code {
        /// Optional language of the code block.
        language: Option<String>,
        /// Code.
        code: String,
        /// Range in the input.
        range: Range<usize>,
    },
}

/// Content of a fragment in a text block.
///
/// Fragment type will matter in further segmentation; e.g., no sentence is
/// split in the middle of a code fragment.
#[derive(Clone, Debug, PartialEq)]
pub enum FragmentContent {
    /// Ordinary text.
    Text(String),
    /// Inline code.
    Code(String),
    /// URL.
    Url(String),
}

impl FragmentContent {
    /// Returns the content as a text.
    pub fn text(&self) -> &String {
        match self {
            Self::Text(text) => text,
            Self::Code(code) => code,
            Self::Url(url) => url,
        }
    }

    /// Returns if it is an ordinary text.
    pub fn is_text(&self) -> bool {
        match self {
            Self::Text(_) => true,
            _ => false,
        }
    }
}

/// Fragment in a text block.
///
/// Second element is the range in the input.
pub type Fragment = (FragmentContent, Range<usize>);

/// Extracts text blocks in a given Markdown text.
///
/// Each of the followings are considered as a text block:
/// - paragraph
///     - split by hard breaks
/// - code block
/// - list item
///
/// Texts in a text block are concatenated.
pub fn extract_text_blocks(text: &str) -> Result<Vec<TextBlock>, Error> {
    let parser = Parser::new_ext(
        text,
        Options::ENABLE_TABLES
            | Options::ENABLE_STRIKETHROUGH
            | Options::ENABLE_TASKLISTS,
    );
    let mut extractor = TextBlockExtractor::new();
    for (event, range) in parser.into_offset_iter() {
        extractor.process_event(event, range)?;
    }
    extractor.finish()
}

#[derive(Debug)]
struct TextBlockExtractor {
    state_stack: Vec<TextBlockExtractorState>,
    text_blocks: Vec<TextBlock>,
}

impl TextBlockExtractor {
    fn new() -> Self {
        let mut state_stack: Vec<TextBlockExtractorState> =
            Vec::with_capacity(10);
        state_stack.push(TextBlockExtractorState::Blank);
        Self {
            state_stack,
            text_blocks: Vec::with_capacity(10),
        }
    }

    fn process_event(
        &mut self,
        event: Event<'_>,
        range: Range<usize>,
    ) -> Result<(), Error> {
        if let Some(state) = self.state_stack.pop() {
            state.process_event(self, event, range)
        } else {
            Err(Error::InvalidContext(format!(
                "Markdown processing is in an undefined state",
            )))
        }
    }

    fn finish(mut self) -> Result<Vec<TextBlock>, Error> {
        if let Some(state) = self.state_stack.pop() {
            match state {
                TextBlockExtractorState::Blank => {
                    if self.state_stack.is_empty() {
                        Ok(self.text_blocks)
                    } else {
                        Err(Error::InvalidContext(format!(
                            "Markdown processing prematurely ended",
                        )))
                    }
                },
                _ => Err(Error::InvalidContext(format!(
                    "Markdown processing prematurely ended",
                ))),
            }
        } else {
            Err(Error::InvalidContext(format!(
                "Markdown processing is in an undefined state",
            )))
        }
    }

    fn process_fragment(&mut self, fragment: Fragment) -> Result<(), Error> {
        if let Some(state) = self.state_stack.pop() {
            state.process_fragment(self, fragment)
        } else {
            Err(Error::InvalidContext(format!(
                "Markdown processing is in an undefined state",
            )))
        }
    }
}

#[derive(Clone, Debug)]
enum TextBlockExtractorState {
    // Blank state.
    //
    // This state is expecting a Markdown content to start.
    Blank,
    // Paragraph state.
    //
    // This state is expecting and collecting contents of a paragraph.
    Paragraph {
        paragraph_type: ParagraphType,
        fragments: Vec<Fragment>,
    },
    // Code block state.
    //
    // This state is expecting a text of a code block.
    CodeBlock {
        language: Option<String>,
        code: Option<String>, // `None` until the first `Event::Text`
        range: Range<usize>,
    },
    // Link state.
    //
    // This state is expecting a text in a link tag.
    Link(Vec<Fragment>),
    // Strikethrough state.
    //
    // This state is expecting a text decorated by a strikethrough.
    Strikethrough,
}

#[derive(Clone, Debug)]
enum ParagraphType {
    // Paragraph.
    Paragraph,
    // List item.
    Item,
}

impl TextBlockExtractorState {
    fn process_event(
        self,
        extractor: &mut TextBlockExtractor,
        event: Event<'_>,
        range: Range<usize>,
    ) -> Result<(), Error> {
        match self {
            Self::Blank => Self::blank_process_event(extractor, event, range),
            Self::Paragraph {
                paragraph_type,
                fragments,
            } => Self::paragraph_process_event(
                paragraph_type,
                fragments,
                extractor,
                event,
                range,
            ),
            Self::CodeBlock {
                language,
                code,
                range: code_range,
            } => Self::code_block_process_event(
                language,
                code,
                code_range,
                extractor,
                event,
            ),
            Self::Link(fragments) => Self::link_process_event(
                fragments,
                extractor,
                event,
                range,
            ),
            Self::Strikethrough => Self::strikethrough_process_event(
                extractor,
                event,
            ),
        }
    }

    fn blank_process_event(
        extractor: &mut TextBlockExtractor,
        event: Event<'_>,
        range: Range<usize>,
    ) -> Result<(), Error> {
        match event {
            Event::Start(Tag::Paragraph) => {
                extractor.state_stack.push(Self::Blank);
                extractor.state_stack.push(Self::Paragraph {
                    paragraph_type: ParagraphType::Paragraph,
                    fragments: Vec::with_capacity(10),
                });
                Ok(())
            },
            Event::Start(Tag::CodeBlock(kind)) => {
                extractor.state_stack.push(Self::Blank);
                let language = match kind {
                    CodeBlockKind::Fenced(language) =>
                        Some(language.into_string()),
                    CodeBlockKind::Indented => None,
                };
                extractor.state_stack.push(Self::CodeBlock {
                    language,
                    code: None,
                    range,
                });
                Ok(())
            },
            Event::Start(Tag::BlockQuote) => {
                extractor.state_stack.push(Self::Blank);
                // processes a nested Markdown structure
                extractor.state_stack.push(Self::Blank);
                Ok(())
            },
            Event::End(Tag::BlockQuote) => Ok(()),
            Event::Start(Tag::List(_)) => {
                extractor.state_stack.push(Self::Blank);
                // processes a nested Markdown structure
                extractor.state_stack.push(Self::Blank);
                Ok(())
            },
            Event::End(Tag::List(_)) => Ok(()),
            Event::Start(Tag::Item) => {
                extractor.state_stack.push(Self::Blank);
                extractor.state_stack.push(Self::Paragraph {
                    paragraph_type: ParagraphType::Item,
                    fragments: Vec::with_capacity(10),
                });
                Ok(())
            },
            _ => Err(Error::InvalidContext(format!(
                "Markdown content must start but got {:?}",
                event,
            ))),
        }
    }

    fn paragraph_process_event(
        paragraph_type: ParagraphType,
        mut fragments: Vec<Fragment>,
        extractor: &mut TextBlockExtractor,
        event: Event<'_>,
        range: Range<usize>,
    ) -> Result<(), Error> {
        // pushes the updated paragraph state to the stack again.
        macro_rules! stack_again {
            () => {
                extractor.state_stack.push(Self::Paragraph {
                    paragraph_type,
                    fragments,
                });
            };
        }

        match event {
            Event::End(Tag::Paragraph) => {
                match paragraph_type {
                    ParagraphType::Paragraph => {
                        extractor.text_blocks.push(TextBlock::Text(fragments));
                        Ok(())
                    },
                    _ => Err(Error::InvalidContext(format!(
                        "paragraph end is expected but got {:?}",
                        event,
                    ))),
                }
            },
            Event::End(Tag::Item) => {
                match paragraph_type {
                    ParagraphType::Item => {
                        extractor.text_blocks.push(TextBlock::Text(fragments));
                        Ok(())
                    },
                    _ => return Err(Error::InvalidContext(format!(
                        "item end is expected but got {:?}",
                        event,
                    ))),
                }
            },
            Event::HardBreak => {
                // ends the current paragraph and starts the new one
                extractor.text_blocks.push(TextBlock::Text(fragments));
                extractor.state_stack.push(Self::Paragraph {
                    paragraph_type,
                    fragments: Vec::with_capacity(10),
                });
                Ok(())
            },
            Event::Text(text) => {
                // concatenates contiguous text fragments
                // otherwise, pushes a new fragment
                if let Some(last_text) = fragments
                    .last_mut()
                    .filter(|(f, _)| f.is_text())
                {
                    last_text.0 = FragmentContent::Text(format!(
                        "{}{}",
                        last_text.0.text(),
                        text.into_string(),
                    ));
                    last_text.1.end = range.end;
                } else {
                    fragments.push((
                        FragmentContent::Text(text.into_string()),
                        range,
                    ));
                }
                stack_again!();
                Ok(())
            },
            Event::Code(code) | Event::Html(code) => {
                fragments.push((
                    FragmentContent::Code(code.into_string()),
                    range,
                ));
                stack_again!();
                Ok(())
            },
            Event::Start(Tag::Link(_, _, _)) => {
                stack_again!();
                extractor.state_stack.push(Self::Link(Vec::with_capacity(10)));
                Ok(())
            },
            Event::Start(Tag::Strikethrough) => {
                stack_again!();
                extractor.state_stack.push(Self::Strikethrough);
                Ok(())
            },
            Event::Start(Tag::Strong)
            | Event::End(Tag::Strong)
            | Event::Start(Tag::Emphasis)
            | Event::End(Tag::Emphasis) => {
                // decoration does not matter
                stack_again!();
                Ok(())
            },
            Event::SoftBreak => {
                // appends a line break to the last fragment
                // unless it is a code fragment
                if let Some(last_text) = fragments
                    .last_mut()
                    .filter(|(f, _)| f.is_text())
                {
                    last_text.0 = FragmentContent::Text(format!(
                        "{}\n",
                        last_text.0.text(),
                    ));
                }
                stack_again!();
                Ok(())
            },
            event => Err(Error::InvalidContext(format!(
                "not implemented yet: {:?}",
                event,
            ))),
        }
    }

    fn code_block_process_event(
        language: Option<String>,
        code: Option<String>,
        code_range: Range<usize>,
        extractor: &mut TextBlockExtractor,
        event: Event<'_>,
    ) -> Result<(), Error> {
        match event {
            Event::End(Tag::CodeBlock(_)) => {
                if let Some(code) = code {
                    extractor.text_blocks.push(TextBlock::Code {
                        language,
                        code,
                        range: code_range,
                    });
                    Ok(())
                } else {
                    Err(Error::InvalidContext(format!(
                        "code block must have a code",
                    )))
                }
            },
            Event::Text(new_code) => {
                if code.is_none() {
                    extractor.state_stack.push(Self::CodeBlock {
                        language,
                        code: Some(new_code.into_string()),
                        range: code_range,
                    });
                    Ok(())
                } else {
                    Err(Error::InvalidContext(format!(
                        "code block has multiple code",
                    )))
                }
            },
            _ => Err(Error::InvalidContext(format!(
                "not implemented yet: {:?}",
                event,
            ))),
        }
    }

    fn link_process_event(
        mut fragments: Vec<Fragment>,
        extractor: &mut TextBlockExtractor,
        event: Event<'_>,
        range: Range<usize>,
    ) -> Result<(), Error> {
        match event {
            Event::End(Tag::Link(_, url, title)) => {
                // replaced with `url` or `title`
                // if the link tag has no contents.
                // `title` precedes `url` unless it is empty
                if fragments.is_empty() {
                    if title.is_empty() {
                        fragments.push((
                            FragmentContent::Url(url.into_string()),
                            range,
                        ));
                    } else {
                        fragments.push((
                            FragmentContent::Text(title.into_string()),
                            range,
                        ));
                    }
                }
                for fragment in fragments {
                    extractor.process_fragment(fragment)?;
                }
                Ok(())
            },
            Event::Text(text) => {
                fragments.push((
                    FragmentContent::Text(text.into_string()),
                    range,
                ));
                extractor.state_stack.push(Self::Link(fragments));
                Ok(())
            },
            Event::Code(code) => {
                fragments.push((
                    FragmentContent::Code(code.into_string()),
                    range,
                ));
                extractor.state_stack.push(Self::Link(fragments));
                Ok(())
            },
            _ => Err(Error::InvalidContext(format!(
                "not implemented yet: {:?}",
                event,
            ))),
        }
    }

    fn strikethrough_process_event(
        extractor: &mut TextBlockExtractor,
        event: Event<'_>,
    ) -> Result<(), Error> {
        match event {
            Event::End(Tag::Strikethrough) => Ok(()),
            Event::Text(_) | Event::Code(_) => {
                extractor.state_stack.push(Self::Strikethrough);
                Ok(())
            },
            _ => Err(Error::InvalidContext(format!(
                "not allowed in strikethrough: {:?}",
                event,
            ))),
        }
    }

    fn process_fragment(
        self,
        extractor: &mut TextBlockExtractor,
        fragment: Fragment,
    ) -> Result<(), Error> {
        match self {
            Self::Paragraph { paragraph_type, fragments } => {
                Self::paragraph_process_fragment(
                    paragraph_type,
                    fragments,
                    extractor,
                    fragment,
                );
                Ok(())
            },
            _ => Err(Error::InvalidContext(format!(
                "nested fragment is not allowed in {:?}",
                self,
            ))),
        }
    }

    fn paragraph_process_fragment(
        paragraph_type: ParagraphType,
        mut fragments: Vec<Fragment>,
        extractor: &mut TextBlockExtractor,
        fragment: Fragment,
    ) {
        match &fragment.0 {
            FragmentContent::Text(text) => {
                // concatenates contiguous text fragments
                // otherwise, pushes a new fragment
                if let Some(last_text) = fragments
                    .last_mut()
                    .filter(|(f, _)| f.is_text())
                {
                    last_text.0 = FragmentContent::Text(format!(
                        "{}{}",
                        last_text.0.text(),
                        text,
                    ));
                    last_text.1.end = fragment.1.end;
                } else {
                    fragments.push(fragment);
                }
            },
            _ => fragments.push(fragment),
        };
        extractor.state_stack.push(Self::Paragraph {
            paragraph_type,
            fragments,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_text_blocks_can_extract_from_plain_text() {
        let input = "simple text";
        assert_eq!(extract_text_blocks(input).unwrap(), vec![
            TextBlock::Text(vec![
                (FragmentContent::Text("simple text".to_string()), 0..11),
            ]),
        ]);
    }

    #[test]
    fn extract_text_blocks_can_extract_from_text_including_html_node() {
        let input = "<unnamed> panicked at";
        assert_eq!(extract_text_blocks(input).unwrap(), vec![
            TextBlock::Text(vec![
                (FragmentContent::Code("<unnamed>".to_string()), 0..9),
                (FragmentContent::Text(" panicked at".to_string()), 9..21),
            ]),
        ]);
    }
}
