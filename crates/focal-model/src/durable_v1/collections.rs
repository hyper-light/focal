use super::{Ref, V1, Value};
use serde::de::{MapAccess, SeqAccess, Visitor};
use serde::ser::{SerializeMap, SerializeSeq, SerializeTuple};
use serde::{Deserialize, Deserializer, Serializer};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::marker::PhantomData;

impl<T: V1> V1 for Option<T> {
    fn serialize_v1<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Some(value) => serializer.serialize_some(&Ref(value)),
            None => serializer.serialize_none(),
        }
    }

    fn deserialize_v1<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Ok(Option::<Value<T>>::deserialize(deserializer)?.map(|value| value.0))
    }
}

impl<T: V1> V1 for Vec<T> {
    #[inline]
    fn serialize_v1<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        T::serialize_sequence_v1(self, serializer)
    }

    fn deserialize_v1<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Values<T>(PhantomData<T>);
        impl<'de, T: V1> Visitor<'de> for Values<T> {
            type Value = Vec<T>;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a V1 sequence")
            }

            fn visit_seq<A: SeqAccess<'de>>(self, mut access: A) -> Result<Self::Value, A::Error> {
                let mut values = Vec::new();
                // The serialized count is untrusted. Allocate only for elements
                // actually decoded, never the count or an unbounded size_hint.
                while let Some(Value(value)) = access.next_element::<Value<T>>()? {
                    values
                        .try_reserve(1)
                        .map_err(|_| serde::de::Error::custom("V1 sequence allocation failed"))?;
                    values.push(value);
                }
                Ok(values)
            }
        }
        deserializer.deserialize_seq(Values(PhantomData))
    }
}

impl<T: V1 + Ord> V1 for BTreeSet<T> {
    fn serialize_v1<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_seq(Some(self.len()))?;
        for value in self {
            seq.serialize_element(&Ref(value))?;
        }
        seq.end()
    }

    fn deserialize_v1<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Values<T>(PhantomData<T>);
        impl<'de, T: V1 + Ord> Visitor<'de> for Values<T> {
            type Value = BTreeSet<T>;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a V1 ordered set")
            }

            fn visit_seq<A: SeqAccess<'de>>(self, mut access: A) -> Result<Self::Value, A::Error> {
                let mut values = BTreeSet::new();
                while let Some(Value(value)) = access.next_element::<Value<T>>()? {
                    // Preserve V1's collection semantics, including de-duplication
                    // during decode. Admission is not a codec responsibility.
                    values.insert(value);
                }
                Ok(values)
            }
        }
        deserializer.deserialize_seq(Values(PhantomData))
    }
}

impl<K: V1 + Ord, V: V1> V1 for BTreeMap<K, V> {
    fn serialize_v1<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(self.len()))?;
        for (key, value) in self {
            map.serialize_entry(&Ref(key), &Ref(value))?;
        }
        map.end()
    }

    fn deserialize_v1<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Values<K, V>(PhantomData<(K, V)>);
        impl<'de, K: V1 + Ord, V: V1> Visitor<'de> for Values<K, V> {
            type Value = BTreeMap<K, V>;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a V1 ordered map")
            }

            fn visit_map<A: MapAccess<'de>>(self, mut access: A) -> Result<Self::Value, A::Error> {
                let mut values = BTreeMap::new();
                while let Some((Value(key), Value(value))) =
                    access.next_entry::<Value<K>, Value<V>>()?
                {
                    // Each row moves directly into the final map. As in the
                    // original decoder, the last duplicate key wins.
                    values.insert(key, value);
                }
                Ok(values)
            }
        }
        deserializer.deserialize_map(Values(PhantomData))
    }
}

impl<A: V1, B: V1> V1 for (A, B) {
    fn serialize_v1<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut tuple = serializer.serialize_tuple(2)?;
        tuple.serialize_element(&Ref(&self.0))?;
        tuple.serialize_element(&Ref(&self.1))?;
        tuple.end()
    }

    fn deserialize_v1<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let (first, second) = <(Value<A>, Value<B>)>::deserialize(deserializer)?;
        Ok((first.0, second.0))
    }
}

impl<T: V1> V1 for Box<T> {
    fn serialize_v1<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.as_ref().serialize_v1(serializer)
    }

    fn deserialize_v1<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        // As with the original Box decoder, allocate the final box only after
        // its value decoded successfully. No second box or model clone is built.
        T::deserialize_v1(deserializer).map(Box::new)
    }
}
