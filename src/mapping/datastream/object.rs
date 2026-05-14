// This file is part of Astarte.
//
// Copyright 2025, 2026 SECO Mind Srl
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

//! Mappings for Datastream with object aggregation
//!
//! Data sent on an object interface is grouped and sent together in a single message.

use std::cmp::Ordering;

use crate::{
    mapping::{
        endpoint::{Endpoint, Level},
        InterfaceMapping, MappingError,
    },
    schema::{Mapping, MappingType},
    MappingPath,
};

/// The mapping of an object must have at least two components.
///
/// See <https://docs.astarte-platform.org/astarte/latest/030-interface.html#endpoints-and-aggregation>.
pub const MIN_OBJECT_ENDPOINT_LEN: usize = 2;

/// Shared struct for a mapping for all interface types.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatastreamObjectMapping {
    pub(crate) endpoint: Endpoint<String>,
    pub(crate) mapping_type: MappingType,
    pub(crate) required: bool,
    #[cfg(feature = "doc-fields")]
    pub(crate) description: Option<String>,
    #[cfg(feature = "doc-fields")]
    pub(crate) doc: Option<String>,
}

impl DatastreamObjectMapping {
    /// Flag to specify if the mapping is required.
    ///
    /// Object Aggregates mappings are optional by default, with this flag you can mark it as
    /// required.
    pub fn required(&self) -> bool {
        self.required
    }

    /// Compares the object field with the mapping endpoint.
    ///
    /// Returns the ordering of the mappings.
    pub fn cmp_object_field(&self, path: &str) -> Ordering {
        let last = self.endpoint.last();

        debug_assert!(
            last.is_some(),
            "an endpoint should always have at least an endpoint"
        );

        match last {
            Some(Level::Simple(simple)) => simple.as_str().cmp(path),
            Some(Level::Parameter(_)) => Ordering::Equal,
            None => Ordering::Less,
        }
    }

    /// Compares the object field with the mapping endpoint.
    ///
    /// Returns true if the last level is equal to the object field.
    pub fn eq_object_field(&self, path: &str) -> bool {
        let last = self.endpoint.last();

        debug_assert!(
            last.is_some(),
            "an endpoint should always have at least an endpoint"
        );

        last.is_some_and(|endpoint_level| *endpoint_level == path)
    }

    pub(crate) fn is_object_path<'a>(&self, path: &MappingPath<'a>) -> bool {
        // Must have the same size -1.
        if self.endpoint.len().saturating_sub(1) != path.len() {
            return false;
        }

        // This will skip the last one for the endpoint for the check above
        self.endpoint
            .iter()
            .zip(path.levels.iter())
            .all(|(endpoint_level, path_level)| match endpoint_level {
                Level::Simple(level) => level == path_level,
                Level::Parameter(_) => true,
            })
    }

    /// Check that two endpoints are compatible with the same object.
    ///
    // https://docs.astarte-platform.org/astarte/latest/030-interface.html#endpoints-and-aggregation
    pub(crate) fn is_same_object(&self, other: &DatastreamObjectMapping) -> bool {
        if self.endpoint.len() != other.endpoint.len() {
            return false;
        }

        // Iterate over the levels of the two endpoints, except the last one that is the object key.
        self.endpoint
            .iter()
            .zip(other.endpoint.iter())
            .rev()
            .skip(1)
            .all(|(level, other_level)| level == other_level)
    }
}

impl InterfaceMapping for DatastreamObjectMapping {
    fn endpoint(&self) -> &Endpoint<String> {
        &self.endpoint
    }

    fn mapping_type(&self) -> MappingType {
        self.mapping_type
    }

    #[cfg(feature = "doc-fields")]
    #[cfg_attr(docsrs, doc(cfg(feature = "doc-fields")))]
    fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }

    #[cfg(feature = "doc-fields")]
    #[cfg_attr(docsrs, doc(cfg(feature = "doc-fields")))]
    fn doc(&self) -> Option<&str> {
        self.doc.as_deref()
    }
}

impl<T> TryFrom<Mapping<T>> for DatastreamObjectMapping
where
    T: AsRef<str> + Into<String>,
{
    type Error = MappingError;

    fn try_from(value: Mapping<T>) -> Result<Self, Self::Error> {
        let endpoint = Endpoint::try_from(value.endpoint.as_ref())?;

        if endpoint.len() < MIN_OBJECT_ENDPOINT_LEN {
            return Err(MappingError::TooShortForObject(endpoint.to_string()));
        }

        Ok(Self {
            endpoint,
            mapping_type: value.mapping_type,
            required: value.required.unwrap_or(false),
            #[cfg(feature = "doc-fields")]
            description: value.description.map(T::into),
            #[cfg(feature = "doc-fields")]
            doc: value.doc.map(T::into),
        })
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    fn mock_object_mapping(endpoint: &str) -> DatastreamObjectMapping {
        DatastreamObjectMapping {
            endpoint: endpoint.parse().unwrap(),
            mapping_type: MappingType::Boolean,
            required: false,
            #[cfg(feature = "doc-fields")]
            description: None,
            #[cfg(feature = "doc-fields")]
            doc: None,
        }
    }

    #[test]
    fn getters_success() {
        let description = Some("Object mapping description");
        let doc = Some("Object mapping doc");
        let mapping_type = MappingType::Boolean;
        let mapping = Mapping {
            endpoint: "/object/path",
            mapping_type,
            reliability: None,
            explicit_timestamp: None,
            retention: None,
            expiry: None,
            database_retention_policy: None,
            database_retention_ttl: None,
            allow_unset: None,
            required: None,
            description,
            doc,
        };

        let obj_mapping = DatastreamObjectMapping::try_from(mapping).unwrap();

        let exp = Endpoint::try_from("/object/path").unwrap();
        assert_eq!(*obj_mapping.endpoint(), exp);
        assert_eq!(obj_mapping.mapping_type(), mapping_type);
        #[cfg(feature = "doc-fields")]
        {
            assert_eq!(description, obj_mapping.description());
            assert_eq!(doc, obj_mapping.doc());
        }
    }

    #[test]
    fn mapping_error_to_short() {
        let mapping = Mapping {
            endpoint: "/tooShort",
            mapping_type: MappingType::Boolean,
            reliability: None,
            explicit_timestamp: None,
            retention: None,
            expiry: None,
            database_retention_policy: None,
            database_retention_ttl: None,
            allow_unset: None,
            required: None,
            description: None,
            doc: None,
        };

        let err = DatastreamObjectMapping::try_from(mapping).unwrap_err();
        assert!(matches!(err, MappingError::TooShortForObject(_)));
    }

    #[test]
    fn check_object_path() {
        let endpoint = mock_object_mapping("/%{sensor_id}/boolean_endpoint");

        let path = MappingPath::try_from("/1/boolean_endpoint").unwrap();

        assert!(!endpoint.is_object_path(&path));

        let path = MappingPath::try_from("/1").unwrap();
        assert!(endpoint.is_object_path(&path));
    }

    #[test]
    fn object_field() {
        let endpoint = mock_object_mapping("/%{sensor_id}/boolean_endpoint");

        assert!(endpoint.eq_object_field("boolean_endpoint"));
        assert!(!endpoint.eq_object_field("foo"));
    }

    #[test]
    fn same_object() {
        let endpoint = mock_object_mapping("/base/foo");
        let same = mock_object_mapping("/base/bar");
        let different = mock_object_mapping("/other/bar");

        assert!(endpoint.is_same_object(&same));
        assert!(!endpoint.is_same_object(&different));
    }
}
