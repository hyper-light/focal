use crate::Limits;
use serde::de::{self, DeserializeSeed, MapAccess, SeqAccess, Visitor};
use std::{collections::BTreeSet, fmt};

pub(crate) fn parse(bytes: &[u8], limits: Limits) -> Result<serde_json::Value, ()> {
    let mut parser = serde_json::Deserializer::from_slice(bytes);
    let mut left = limits.max_nodes;
    Shape {
        left: &mut left,
        depth: 0,
        max_depth: limits.max_depth,
    }
    .deserialize(&mut parser)
    .map_err(|_| ())?;
    parser.end().map_err(|_| ())?;
    serde_json::from_slice(bytes).map_err(|_| ())
}
struct Shape<'a> {
    left: &'a mut usize,
    depth: usize,
    max_depth: usize,
}
impl<'de> DeserializeSeed<'de> for Shape<'_> {
    type Value = ();
    fn deserialize<D: de::Deserializer<'de>>(self, d: D) -> Result<(), D::Error> {
        *self.left = self
            .left
            .checked_sub(1)
            .ok_or_else(|| de::Error::custom("node bound"))?;
        if self.depth > self.max_depth {
            return Err(de::Error::custom("depth bound"));
        }
        d.deserialize_any(self)
    }
}
impl<'de> Visitor<'de> for Shape<'_> {
    type Value = ();
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("bounded JSON")
    }
    fn visit_bool<E: de::Error>(self, _: bool) -> Result<(), E> {
        Ok(())
    }
    fn visit_i64<E: de::Error>(self, _: i64) -> Result<(), E> {
        Ok(())
    }
    fn visit_u64<E: de::Error>(self, _: u64) -> Result<(), E> {
        Ok(())
    }
    fn visit_f64<E: de::Error>(self, _: f64) -> Result<(), E> {
        Ok(())
    }
    fn visit_str<E: de::Error>(self, _: &str) -> Result<(), E> {
        Ok(())
    }
    fn visit_unit<E: de::Error>(self) -> Result<(), E> {
        Ok(())
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut a: A) -> Result<(), A::Error> {
        let depth = self
            .depth
            .checked_add(1)
            .ok_or_else(|| de::Error::custom("depth bound"))?;
        while a
            .next_element_seed(Shape {
                left: self.left,
                depth,
                max_depth: self.max_depth,
            })?
            .is_some()
        {}
        Ok(())
    }
    fn visit_map<A: MapAccess<'de>>(self, mut a: A) -> Result<(), A::Error> {
        let depth = self
            .depth
            .checked_add(1)
            .ok_or_else(|| de::Error::custom("depth bound"))?;
        let mut keys = BTreeSet::new();
        while let Some(key) = a.next_key::<String>()? {
            if !keys.insert(key) {
                return Err(de::Error::custom("duplicate key"));
            }
            a.next_value_seed(Shape {
                left: self.left,
                depth,
                max_depth: self.max_depth,
            })?;
        }
        Ok(())
    }
}
