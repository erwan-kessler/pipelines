use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::io;
use std::io::{BufRead};
use std::str::FromStr;
use anyhow::{anyhow, Error};
use tracing::{debug, error, Level};

#[repr(u8)]
#[derive(Debug)]
pub enum Encoding {
    Ascii = 0,
    Hex = 1,
}

impl TryFrom<u8> for Encoding {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        Ok(match value {
            0 => Self::Ascii,
            1 => Self::Hex,
            _ => { return Err(anyhow!("Not a valid encoding")); }
        })
    }
}

impl Encoding {
    pub fn decode(&self, msg: String) -> anyhow::Result<String> {
        Ok(match self {
            Encoding::Ascii => {
                msg
            }
            Encoding::Hex => {
                String::from_utf8(hex::decode(msg.as_str())?)?
            }
        })
    }
}

#[derive(Debug, Clone)]
pub struct Message {
    id: u8,
    body: String,
}

impl TryFrom<(u8, Encoding, String)> for Message {
    type Error = Error;

    fn try_from((id, encoding, msg): (u8, Encoding, String)) -> Result<Self, Self::Error> {
        Ok(Message {
            id,
            body: encoding.decode(msg)?,
        })
    }
}

impl Eq for Message {}

impl PartialEq<Self> for Message {
    fn eq(&self, other: &Self) -> bool {
        self.id.eq(&other.id)
    }
}

impl PartialOrd<Self> for Message {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.id.partial_cmp(&other.id)
    }
}

impl Ord for Message {
    fn cmp(&self, other: &Self) -> Ordering {
        self.id.cmp(&other.id)
    }
}

#[derive(Debug)]
pub struct ParsedMessage {
    pipeline_id: u8,
    id: u8,
    encoding: Encoding,
    message: String,
    next_id: Option<u8>,
}

impl ParsedMessage {
    fn parse(line: &str) -> anyhow::Result<Self> {
        let mut tokens = line.split(" ");

        let pipeline_id = tokens.next().ok_or(anyhow::anyhow!("Missing pipeline id"))?;
        let pipeline_id = u8::from_str(pipeline_id)?;

        let id = tokens.next().ok_or(anyhow::anyhow!("Missing id"))?;
        let id = u8::from_str(id)?;

        let encoding = tokens.next().ok_or(anyhow::anyhow!("Missing id"))?;
        let encoding: Encoding = u8::from_str(encoding)?.try_into()?;

        let message = tokens.next().ok_or(anyhow::anyhow!("Missing msg"))?;
        let message = message.to_string();

        let next_id = tokens.next().ok_or(anyhow::anyhow!("Missing next_id"))?;
        let next_id = i16::from_str(next_id)?;
        let next_id = match next_id {
            -1 => None,
            x if x >= 0 && x <= u8::MAX as i16 => Some(x as u8),
            _ => {
                return Err(anyhow!("Incorrect next id {}",next_id));
            }
        };

        Ok(Self {
            pipeline_id,
            id,
            encoding,
            message,
            next_id,
        })
    }
}

pub type PipelineId = u8;

#[derive(Default, Debug, Clone)]
pub struct Pipeline {
    id: PipelineId,
    next_id: Option<u8>,
    closed: bool,
    message: BinaryHeap<Message>,
}

impl Hash for Pipeline {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl Pipeline {
    pub fn new(id: PipelineId) -> Self {
        Self {
            id,
            ..Default::default()
        }
    }
}
#[derive(Default, Clone)]
pub struct PipelinesConfig{
    pub discard_invalid_next_id:bool,
}

#[derive(Default, Clone)]
pub struct Pipelines {
    inner: HashMap<PipelineId, Pipeline>,
    config:PipelinesConfig,
}

impl Pipelines {
    pub fn display<W:std::fmt::Write>(self, writer: &mut W) -> std::fmt::Result {
        let mut keys = self.inner.keys().collect::<Vec<_>>();
        keys.sort_unstable();
        for key in keys {
            match self.inner.get(&key) {
                None => {
                    error!("Pipelines hashmap was modified in between")
                }
                Some(pipeline) => {
                    writeln!(writer, "Pipeline:{}", pipeline.id)?;
                    for msg in pipeline.message.clone().into_sorted_vec() {
                        writeln!(writer, "\t{}| {}", msg.id, msg.body)?;
                    }
                }
            }
        }
        Ok(())
    }
}

impl Display for Pipelines {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.clone().display(f)
    }
}

impl Pipelines {
    pub fn new(config:PipelinesConfig) -> Self {
        Self {
            inner: HashMap::with_capacity(1024),
            config,
        }
    }

    pub fn insert_message(&mut self, msg: ParsedMessage) {
        let pipeline = self.inner.entry(msg.pipeline_id)
            .or_insert(Pipeline::new(msg.pipeline_id));
        if pipeline.closed {
            debug!("The following message was ignored because the pipeline was closed: {msg:?}");
            return;
        }
        if let Some(next_id) = &pipeline.next_id {
            if msg.id != *next_id && self.config.discard_invalid_next_id {
                debug!("Message {msg:?} was ignored because it's not supposed to be received, should have been id {next_id}");
                return;
            }
        }
        match (msg.id, msg.encoding, msg.message).try_into() {
            Ok(msg) => pipeline.message.push(msg),
            Err(e) => {
                debug!("Message is not valid {e:?}");
            }
        }
        pipeline.next_id = msg.next_id;
        if msg.next_id.is_none() {
            // close the pipeline
            pipeline.closed = true;
        }
    }
}


fn main() {
    tracing_subscriber::fmt()
        .with_max_level(Level::TRACE)
        .compact()
        .init();
    let stdin = io::stdin();
    let mut lines = stdin.lock().lines();
    let mut pipelines = Pipelines::new(PipelinesConfig::default());
    while let Some(Ok(line)) = lines.next() {
        if line.is_empty() {
            break;
        }
        match ParsedMessage::parse(line.as_str()) {
            Ok(msg) => { pipelines.insert_message(msg); }
            Err(err) => {
                debug!("Could not parse line `{line}` with err: {err:?}");
            }
        }
    }
}


#[test_log::test]
fn test() {
    let lines = r#"2 1 1 4F4B 5
1 0 0 some_text 1
1 1 0 another_text 3
2 5 1 4F4B -1
"#;
    let mut pipelines = Pipelines::new(Default::default());
    for line in lines.lines() {
        match ParsedMessage::parse(line) {
            Ok(msg) => { pipelines.insert_message(msg); }
            Err(err) => {
                debug!("Could not parse line `{line}` with err: {err:?}");
            }
        }
    }
    println!("{}", pipelines);
}

#[test_log::test]
fn test_simple() {
    let lines = r#"3 1 0 message_31 -1
      1 0 0 message_10 1 This text should be ignored
1 3 0 message_13 -1
err
12 8 m...
1 1 0 message_11 2
5 9 0 message_59 10
2 0 0 message_20 2
13 1 1 66616E63792031335F31 2
13 2 1 66616E63792074657874 -1
1 2 0 message_12 3
2 2 0 message_22 -1
1 0 0 message_10_2 1
5 11 0 message_510_2 -1
"#;
    let mut pipelines = Pipelines::new(PipelinesConfig{
        discard_invalid_next_id:false,
    });
    for line in lines.lines() {
        match ParsedMessage::parse(line) {
            Ok(msg) => { pipelines.insert_message(msg); }
            Err(err) => {
                debug!("Could not parse line `{line}` with err: {err:?}");
            }
        }
    }
    let mut s=String::new();
    pipelines.display(&mut s).expect("works");
    println!("{}",s);
}
