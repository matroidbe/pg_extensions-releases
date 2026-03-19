use pgrx::datum::Internal;
use pgrx::prelude::*;
use pgrx::ToAggregateName;
use serde::{Deserialize, Serialize};

use crate::ffi;
use crate::types::bbox3d::BBox3D;
use crate::types::solid::Solid;

// ===== solid_agg_union: boolean union of all solids in a group =====

#[derive(Copy, Clone, Default, Debug, PostgresType, Serialize, Deserialize)]
#[pg_binary_protocol]
pub struct SolidAggUnion;

impl ToAggregateName for SolidAggUnion {
    const NAME: &'static str = "solid_agg_union";
}

#[pg_aggregate]
impl Aggregate<SolidAggUnion> for SolidAggUnion {
    type Args = name!(value, Solid);
    type State = Internal;
    type Finalize = Solid;

    #[pgrx(immutable)]
    fn state(
        mut current: Self::State,
        arg: Self::Args,
        _fcinfo: pg_sys::FunctionCallInfo,
    ) -> Self::State {
        let inner = unsafe { current.get_or_insert_default::<Option<Solid>>() };
        let prev = inner.take();
        *inner = Some(match prev {
            None => arg,
            Some(acc) => {
                let shape_a = acc
                    .to_occt_shape()
                    .unwrap_or_else(|e| pgrx::error!("solid_agg_union: {e}"));
                let shape_b = arg
                    .to_occt_shape()
                    .unwrap_or_else(|e| pgrx::error!("solid_agg_union: {e}"));
                let fused = ffi::boolean::fuse(&shape_a, &shape_b)
                    .unwrap_or_else(|e| pgrx::error!("solid_agg_union: {e}"));
                Solid::from_occt_shape(&fused)
                    .unwrap_or_else(|e| pgrx::error!("solid_agg_union: {e}"))
            }
        });
        current
    }

    fn finalize(
        mut current: Self::State,
        _direct_args: Self::OrderedSetArgs,
        _fcinfo: pg_sys::FunctionCallInfo,
    ) -> Self::Finalize {
        let inner = unsafe { current.get_or_insert_default::<Option<Solid>>() };
        inner
            .take()
            .unwrap_or_else(|| pgrx::error!("solid_agg_union: no solids in aggregate"))
    }
}

// ===== solid_agg_bbox: combined bounding box of all solids =====

#[derive(Copy, Clone, Default, Debug, PostgresType, Serialize, Deserialize)]
#[pg_binary_protocol]
pub struct SolidAggBbox;

impl ToAggregateName for SolidAggBbox {
    const NAME: &'static str = "solid_agg_bbox";
}

#[pg_aggregate]
impl Aggregate<SolidAggBbox> for SolidAggBbox {
    type Args = name!(value, Solid);
    type State = Internal;
    type Finalize = BBox3D;

    #[pgrx(immutable)]
    fn state(
        mut current: Self::State,
        arg: Self::Args,
        _fcinfo: pg_sys::FunctionCallInfo,
    ) -> Self::State {
        let inner = unsafe { current.get_or_insert_default::<Option<BBox3D>>() };
        let arg_bbox = arg.header.bbox3d();
        let prev = inner.take();
        *inner = Some(match prev {
            None => arg_bbox,
            Some(acc) => acc.union(&arg_bbox),
        });
        current
    }

    fn finalize(
        mut current: Self::State,
        _direct_args: Self::OrderedSetArgs,
        _fcinfo: pg_sys::FunctionCallInfo,
    ) -> Self::Finalize {
        let inner = unsafe { current.get_or_insert_default::<Option<BBox3D>>() };
        inner
            .take()
            .unwrap_or_else(|| pgrx::error!("solid_agg_bbox: no solids in aggregate"))
    }
}
