//! Gateway API provider: Gateways of the GatewayClasses with our `controllerName`, and
//! the HTTPRoutes attached to them, become Pingap locations, certificates, plugins and
//! upstreams in the same etcd prefix.
//!
//! - [`api`]: the generated Gateway API types.
//! - [`model`]: the types shared by the parts below.
//! - [`names`], [`translate`], [`extended`], [`tls`]: pure translation of Gateways,
//!   routes, Services and Secrets into the desired entries and statuses.
//! - [`render`], [`plan`], [`writer`]: TOML of the entries, the ordered etcd steps that
//!   keep every intermediate state valid for Pingap, and the etcd I/O.
//! - [`watch`], [`secrets`], [`status`]: the Kubernetes side.
//! - [`controller`]: the loop that ties them together (leader-only writes).

pub mod api;
pub mod controller;
pub mod extended;
pub mod model;
pub mod names;
pub mod plan;
pub mod render;
pub mod secrets;
pub mod status;
pub mod tls;
pub mod translate;
pub mod watch;
pub mod writer;
