//! WMS 1.3.0 `GetCapabilities` XML builder.
//!
//! Enumerates every registered (dataset, surface_var) pair that has a
//! mesh attached and emits one `<Layer>` per pair. Layer name follows
//! the GeoServer convention: `<dataset>:<surface_var>` (colon-
//! separated). Each layer carries its native CRS, the mesh extent as
//! `<BoundingBox>`, and a `<Dimension name="time">` populated from
//! the distinct `chunks.time_range` lower bounds — that's the input
//! QGIS Time Manager / ArcGIS Time Slider feed off.

use std::collections::HashMap;

use pgrx::prelude::*;

use super::xml_escape;

const SUPPORTED_COLORMAPS: &[&str] = &["viridis"];

/// Build the GetCapabilities XML body. Called from the bgworker's
/// main thread — SPI is valid.
pub fn build(_query: &HashMap<String, String>) -> Result<String, String> {
    let layers = enumerate_layers()?;

    let mut xml = String::with_capacity(4096);
    xml.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    xml.push_str(
        "<WMS_Capabilities version=\"1.3.0\" \
         xmlns=\"http://www.opengis.net/wms\" \
         xmlns:xlink=\"http://www.w3.org/1999/xlink\">\n",
    );

    // -------- Service section --------
    xml.push_str("  <Service>\n");
    xml.push_str("    <Name>WMS</Name>\n");
    xml.push_str("    <Title>pg_xarray WMS</Title>\n");
    xml.push_str(
        "    <Abstract>OGC WMS 1.3.0 endpoint served directly by the \
         pg_xarray Postgres extension. Indexed scientific datasets \
         (SELAFIN, NetCDF, Zarr) rendered as 2D raster tiles with \
         bbox + time pruning pushed into the catalog.</Abstract>\n",
    );
    xml.push_str("  </Service>\n");

    // -------- Capability section --------
    xml.push_str("  <Capability>\n");
    xml.push_str("    <Request>\n");
    xml.push_str(
        "      <GetCapabilities>\n        <Format>application/xml</Format>\n      </GetCapabilities>\n",
    );
    xml.push_str("      <GetMap>\n        <Format>image/png</Format>\n      </GetMap>\n");
    xml.push_str("    </Request>\n");
    xml.push_str("    <Exception>\n      <Format>XML</Format>\n    </Exception>\n");

    // Root layer container.
    xml.push_str("    <Layer>\n");
    xml.push_str("      <Title>pg_xarray catalog</Title>\n");
    // Advertise EPSG:0 as a fallback for Cartesian data — every layer
    // overrides this with its own CRS below.
    xml.push_str("      <CRS>EPSG:0</CRS>\n");

    for layer in &layers {
        push_layer(&mut xml, layer);
    }

    xml.push_str("    </Layer>\n");
    xml.push_str("  </Capability>\n");
    xml.push_str("</WMS_Capabilities>\n");

    Ok(xml)
}

fn push_layer(xml: &mut String, layer: &LayerInfo) {
    xml.push_str("      <Layer queryable=\"0\">\n");
    xml.push_str(&format!(
        "        <Name>{}</Name>\n",
        xml_escape(&format!("{}:{}", layer.dataset, layer.variable)),
    ));
    xml.push_str(&format!(
        "        <Title>{}</Title>\n",
        xml_escape(&format!(
            "{} — {} ({})",
            layer.dataset, layer.variable, layer.units
        )),
    ));
    let crs = if layer.srid == 0 {
        "EPSG:0".to_string()
    } else {
        format!("EPSG:{}", layer.srid)
    };
    xml.push_str(&format!("        <CRS>{}</CRS>\n", crs));
    if let Some(b) = &layer.bbox {
        xml.push_str(&format!(
            "        <BoundingBox CRS=\"{crs}\" minx=\"{}\" miny=\"{}\" maxx=\"{}\" maxy=\"{}\"/>\n",
            b.min_x, b.min_y, b.max_x, b.max_y,
        ));
    }
    // EX_GeographicBoundingBox is in degrees (WGS 84). It is only written
    // when the extent could be transformed, i.e. the layer has a real SRID:
    // clients zoom to it, so projected metres here would be worse than none.
    if let Some(g) = &layer.geo_bbox {
        xml.push_str("        <EX_GeographicBoundingBox>\n");
        xml.push_str(&format!(
            "          <westBoundLongitude>{}</westBoundLongitude>\n",
            g.min_x
        ));
        xml.push_str(&format!(
            "          <eastBoundLongitude>{}</eastBoundLongitude>\n",
            g.max_x
        ));
        xml.push_str(&format!(
            "          <southBoundLatitude>{}</southBoundLatitude>\n",
            g.min_y
        ));
        xml.push_str(&format!(
            "          <northBoundLatitude>{}</northBoundLatitude>\n",
            g.max_y
        ));
        xml.push_str("        </EX_GeographicBoundingBox>\n");
    }
    if !layer.times.is_empty() {
        let default = layer.times.first().cloned().unwrap_or_default();
        xml.push_str(&format!(
            "        <Dimension name=\"time\" units=\"ISO8601\" default=\"{}\">{}</Dimension>\n",
            xml_escape(&default),
            xml_escape(&layer.times.join(",")),
        ));
    }
    for cmap in SUPPORTED_COLORMAPS {
        xml.push_str("        <Style>\n");
        xml.push_str(&format!("          <Name>{cmap}</Name>\n"));
        xml.push_str(&format!("          <Title>{cmap} colormap</Title>\n"));
        xml.push_str("        </Style>\n");
    }
    xml.push_str("      </Layer>\n");
}

#[derive(Debug, Clone)]
pub(crate) struct LayerInfo {
    pub dataset: String,
    pub variable: String,
    pub units: String,
    pub srid: i32,
    pub bbox: Option<LayerBbox>,
    /// `bbox` in EPSG:4326 longitude/latitude, when the SRID is known.
    pub geo_bbox: Option<LayerBbox>,
    /// Distinct chunk lower bounds in ISO 8601 — clients use these as
    /// the WMS-T time-dimension values.
    pub times: Vec<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct LayerBbox {
    pub min_x: f64,
    pub min_y: f64,
    pub max_x: f64,
    pub max_y: f64,
}

/// Enumerate every (dataset, variable) row that has at least one chunk
/// tied to a mesh, plus its extent + distinct time-range lower bounds.
fn enumerate_layers() -> Result<Vec<LayerInfo>, String> {
    let layer_sql = r#"
        SELECT d.name                                  AS dataset,
               v.name                                  AS variable,
               COALESCE(v.units, '')                   AS units,
               COALESCE(v.srid, d.default_srid, 4326)  AS srid,
               public.ST_AsText(m.extent)              AS extent_wkt,
               -- the extent in degrees: densified first, so a rotated or
               -- curved outline keeps its true bounds after transforming
               CASE WHEN public.ST_SRID(m.extent) > 0
                         AND EXISTS (SELECT 1 FROM public.spatial_ref_sys s
                                     WHERE s.srid = public.ST_SRID(m.extent))
                    THEN public.ST_AsText(public.ST_Envelope(public.ST_Transform(
                             public.ST_Segmentize(m.extent,
                                 greatest(public.ST_XMax(m.extent) - public.ST_XMin(m.extent),
                                          public.ST_YMax(m.extent) - public.ST_YMin(m.extent),
                                          1e-9) / 32),
                             4326)))
               END                                     AS geo_extent_wkt
        FROM   pgx.chunks       c
        JOIN   pgx.variables    v  ON v.id = c.variable_id
        JOIN   pgx.datasets     d  ON d.id = v.dataset_id
        JOIN   pgx.mesh_versions mv ON mv.id = c.mesh_version_id
        JOIN   pgx.meshes       m  ON m.id  = mv.mesh_id
        GROUP  BY d.name, v.name, v.units, v.srid, d.default_srid, m.extent
        ORDER  BY d.name, v.name
    "#;
    let times_sql = r#"
        SELECT to_char(lower(c.time_range) AT TIME ZONE 'UTC',
                       'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS t_iso
        FROM   pgx.chunks    c
        JOIN   pgx.variables v ON v.id = c.variable_id
        JOIN   pgx.datasets  d ON d.id = v.dataset_id
        WHERE  d.name = $1
          AND  v.name = $2
          AND  c.time_range IS NOT NULL
        GROUP  BY t_iso
        ORDER  BY t_iso
    "#;

    let layers: Result<Vec<LayerInfo>, spi::Error> = Spi::connect(|client| {
        let table = client.select(layer_sql, None, &[])?;
        let mut out = Vec::new();
        for row in table {
            let dataset: String = row.get::<String>(1)?.unwrap_or_default();
            let variable: String = row.get::<String>(2)?.unwrap_or_default();
            let units: String = row.get::<String>(3)?.unwrap_or_default();
            let srid: i32 = row.get::<i32>(4)?.unwrap_or(4326);
            let extent_wkt: Option<String> = row.get::<String>(5)?;
            let geo_wkt: Option<String> = row.get::<String>(6)?;
            let to_bbox = |wkt: Option<String>| {
                wkt.and_then(|w| crate::raster::viewport::WorldBbox::from_wkt(&w))
                    .map(|b| LayerBbox {
                        min_x: b.min_x,
                        min_y: b.min_y,
                        max_x: b.max_x,
                        max_y: b.max_y,
                    })
            };
            out.push(LayerInfo {
                dataset,
                variable,
                units,
                srid,
                bbox: to_bbox(extent_wkt),
                geo_bbox: to_bbox(geo_wkt),
                times: Vec::new(),
            });
        }
        Ok(out)
    });
    let mut layers = layers.map_err(|e| format!("GetCapabilities layer enumeration: {e}"))?;

    for layer in &mut layers {
        let times: Result<Vec<String>, spi::Error> = Spi::connect(|client| {
            let table = client.select(
                times_sql,
                None,
                &[(&layer.dataset).into(), (&layer.variable).into()],
            )?;
            let mut out = Vec::new();
            for row in table {
                if let Some(t) = row.get::<String>(1)? {
                    out.push(t);
                }
            }
            Ok(out)
        });
        if let Ok(t) = times {
            layer.times = t;
        }
    }

    Ok(layers)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn layer(geo: Option<LayerBbox>) -> LayerInfo {
        LayerInfo {
            dataset: "flood".into(),
            variable: "WATER DEPTH".into(),
            units: "M".into(),
            srid: 2154,
            bbox: Some(LayerBbox {
                min_x: 997099.7,
                min_y: 6262285.8,
                max_x: 1006294.8,
                max_y: 6279500.4,
            }),
            geo_bbox: geo,
            times: vec![],
        }
    }

    #[test]
    fn geographic_bbox_is_in_degrees() {
        let mut xml = String::new();
        push_layer(
            &mut xml,
            &layer(Some(LayerBbox {
                min_x: 6.68,
                min_y: 43.41,
                max_x: 6.80,
                max_y: 43.57,
            })),
        );
        assert!(xml.contains("<westBoundLongitude>6.68</westBoundLongitude>"));
        assert!(xml.contains("<northBoundLatitude>43.57</northBoundLatitude>"));
        // the native extent stays in the layer's own CRS
        assert!(xml.contains("<BoundingBox CRS=\"EPSG:2154\" minx=\"997099.7\""));
    }

    #[test]
    fn no_geographic_bbox_without_a_known_srid() {
        let mut xml = String::new();
        push_layer(&mut xml, &layer(None));
        assert!(!xml.contains("EX_GeographicBoundingBox"), "{xml}");
    }
}
