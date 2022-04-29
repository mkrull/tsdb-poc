use serde::{Deserialize, Serialize};
use std::cmp::{Eq, PartialEq};
use std::{fs::read_to_string, path::Path};

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct BlockStats {
    #[serde(rename = "numSamples")]
    pub num_samples: u64,
    #[serde(rename = "numSeries")]
    pub num_series: u64,
    #[serde(rename = "numChunks")]
    pub num_chunks: u64,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct BlockCompaction {
    pub level: u8,
    pub sources: Vec<String>,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct MetaData {
    pub version: u8,
    pub ulid: String, // TODO use ulid type
    #[serde(rename = "minTime")]
    pub min_time: u64,
    #[serde(rename = "maxTime")]
    pub max_time: u64,
    pub stats: BlockStats,
    pub compaction: BlockCompaction,
}

impl MetaData {
    pub fn new(path: &Path) -> Self {
        let content =
            read_to_string(path).expect("Something went wrong reading the test meta.json file");

        let m: MetaData = serde_json::from_str(&content).expect("Failed to deserialize meta.json");

        m
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn load_meta_data() {
        let meta = MetaData::new(Path::new("testdata/testblock/meta.json"));

        let expected = MetaData {
            version: 1,
            ulid: String::from("01G0P9P6T0VSA5Z484BRGH2N68"),
            min_time: 1650005003777,
            max_time: 1650009600000,
            stats: BlockStats {
                num_samples: 5551013,
                num_series: 35354,
                num_chunks: 37020,
            },
            compaction: BlockCompaction {
                level: 1,
                sources: [String::from("01G0P9P6T0VSA5Z484BRGH2N68")].to_vec(),
            },
        };

        assert_eq!(meta, expected);

        let ser = serde_json::to_string_pretty(&meta).unwrap();
        let expected = r#"{
  "version": 1,
  "ulid": "01G0P9P6T0VSA5Z484BRGH2N68",
  "minTime": 1650005003777,
  "maxTime": 1650009600000,
  "stats": {
    "numSamples": 5551013,
    "numSeries": 35354,
    "numChunks": 37020
  },
  "compaction": {
    "level": 1,
    "sources": [
      "01G0P9P6T0VSA5Z484BRGH2N68"
    ]
  }
}"#;

        assert_eq!(expected, ser);
    }
}
