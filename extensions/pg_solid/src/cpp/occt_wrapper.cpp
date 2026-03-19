// pg_solid OCCT C++ wrapper
// Thin extern "C" layer over OpenCASCADE C++ API.
// All functions catch C++ exceptions and return error codes.
// Return: 0 = success, non-zero = error. Call occt_last_error() for message.

#include <cstring>
#include <cstdlib>
#include <sstream>
#include <string>
#include <unistd.h>

// OCCT headers
#include <BRep_Builder.hxx>
#include <BRepBndLib.hxx>
#include <BRepBuilderAPI_MakeEdge.hxx>
#include <BRepBuilderAPI_MakeFace.hxx>
#include <BRepBuilderAPI_MakeWire.hxx>
#include <BRepCheck_Analyzer.hxx>
#include <BRepGProp.hxx>
#include <BRepPrimAPI_MakeBox.hxx>
#include <BRepPrimAPI_MakeCone.hxx>
#include <BRepPrimAPI_MakeCylinder.hxx>
#include <BRepPrimAPI_MakeSphere.hxx>
#include <BRepTools.hxx>
#include <Bnd_Box.hxx>
#include <Bnd_OBB.hxx>
#include <GProp_GProps.hxx>
#include <TopExp_Explorer.hxx>
#include <TopoDS.hxx>
#include <TopoDS_Shape.hxx>
#include <TopAbs_ShapeEnum.hxx>
#include <Standard_Failure.hxx>
#include <BRepExtrema_DistShapeShape.hxx>
#include <BRepClass3d_SolidClassifier.hxx>
#include <TopAbs_State.hxx>
#include <BRepBuilderAPI_Transform.hxx>
#include <gp_Trsf.hxx>
#include <gp_Vec.hxx>
#include <gp_Ax1.hxx>
#include <gp_Dir.hxx>
#include <gp_Pnt.hxx>
// Phase 3: Boolean operations, healing, STL export, extrusion
#include <BRepAlgoAPI_Fuse.hxx>
#include <BRepAlgoAPI_Cut.hxx>
#include <BRepAlgoAPI_Common.hxx>
#include <ShapeFix_Shape.hxx>
#include <BRepMesh_IncrementalMesh.hxx>
#include <BRep_Tool.hxx>
#include <Poly_Triangulation.hxx>
#include <TopLoc_Location.hxx>
#include <BRepPrimAPI_MakePrism.hxx>
// Phase 6: Check, offset, section
#include <gp_Pln.hxx>
#include <BRepCheck_Result.hxx>
#include <BRepCheck_ListOfStatus.hxx>
#include <BRepOffsetAPI_MakeOffsetShape.hxx>
#include <BRepAlgoAPI_Section.hxx>
#include <ShapeAnalysis_FreeBounds.hxx>
#include <TopTools_HSequenceOfShape.hxx>
// Phase 7: HLR 2D projection
#include <HLRBRep_Algo.hxx>
#include <HLRBRep_HLRToShape.hxx>
#include <HLRAlgo_Projector.hxx>
// IFC geometry construction
#include <Geom_Circle.hxx>
#include <TopoDS_Compound.hxx>
#include <BRepBuilderAPI_Sewing.hxx>
// STEP and IGES import/export
#include <STEPControl_Reader.hxx>
#include <STEPControl_Writer.hxx>
#include <IGESControl_Reader.hxx>
#include <IGESControl_Writer.hxx>
#include <IFSelect_ReturnStatus.hxx>

// Thread-local error message buffer
static thread_local std::string g_last_error;

static void set_error(const char* msg) {
    g_last_error = msg;
}

static void set_error(const std::string& msg) {
    g_last_error = msg;
}

// Wrap a TopoDS_Shape on the heap so we can pass opaque pointers through FFI
static TopoDS_Shape* to_heap(const TopoDS_Shape& shape) {
    return new TopoDS_Shape(shape);
}

static TopoDS_Shape* as_shape(void* ptr) {
    return static_cast<TopoDS_Shape*>(ptr);
}

extern "C" {

const char* occt_last_error() {
    return g_last_error.c_str();
}

void occt_shape_free(void* shape) {
    delete as_shape(shape);
}

void occt_buffer_free(uint8_t* buf) {
    free(buf);
}

// ---- BRep serialization ----

int occt_brep_read(const uint8_t* data, size_t len, void** shape_out) {
    try {
        std::string brep_str(reinterpret_cast<const char*>(data), len);
        std::istringstream iss(brep_str);

        BRep_Builder builder;
        TopoDS_Shape shape;
        // OCCT 7.8: BRepTools::Read returns void, throws on failure
        BRepTools::Read(shape, iss, builder);
        if (shape.IsNull()) {
            set_error("BRepTools::Read failed: invalid BRep data");
            return 1;
        }
        *shape_out = to_heap(shape);
        return 0;
    } catch (Standard_Failure& e) {
        set_error(e.GetMessageString());
        return 1;
    } catch (std::exception& e) {
        set_error(e.what());
        return 1;
    } catch (...) {
        set_error("unknown C++ exception in occt_brep_read");
        return 1;
    }
}

int occt_brep_write(void* shape, uint8_t** data_out, size_t* len_out) {
    try {
        std::ostringstream oss;
        BRepTools::Write(*as_shape(shape), oss);
        std::string brep_str = oss.str();

        *len_out = brep_str.size();
        *data_out = static_cast<uint8_t*>(malloc(brep_str.size()));
        if (!*data_out) {
            set_error("malloc failed in occt_brep_write");
            return 1;
        }
        memcpy(*data_out, brep_str.data(), brep_str.size());
        return 0;
    } catch (Standard_Failure& e) {
        set_error(e.GetMessageString());
        return 1;
    } catch (std::exception& e) {
        set_error(e.what());
        return 1;
    } catch (...) {
        set_error("unknown C++ exception in occt_brep_write");
        return 1;
    }
}

// ---- Bounding box ----

int occt_shape_bbox(void* shape, float* xmin, float* ymin, float* zmin,
                    float* xmax, float* ymax, float* zmax) {
    try {
        Bnd_Box box;
        BRepBndLib::Add(*as_shape(shape), box);
        if (box.IsVoid()) {
            set_error("bounding box is void (empty shape)");
            return 1;
        }
        double x1, y1, z1, x2, y2, z2;
        box.Get(x1, y1, z1, x2, y2, z2);
        *xmin = static_cast<float>(x1);
        *ymin = static_cast<float>(y1);
        *zmin = static_cast<float>(z1);
        *xmax = static_cast<float>(x2);
        *ymax = static_cast<float>(y2);
        *zmax = static_cast<float>(z2);
        return 0;
    } catch (Standard_Failure& e) {
        set_error(e.GetMessageString());
        return 1;
    } catch (...) {
        set_error("unknown C++ exception in occt_shape_bbox");
        return 1;
    }
}

// ---- Properties ----

int occt_shape_volume(void* shape, double* volume_out) {
    try {
        GProp_GProps props;
        BRepGProp::VolumeProperties(*as_shape(shape), props);
        *volume_out = props.Mass();
        return 0;
    } catch (Standard_Failure& e) {
        set_error(e.GetMessageString());
        return 1;
    } catch (...) {
        set_error("unknown C++ exception in occt_shape_volume");
        return 1;
    }
}

int occt_shape_surface_area(void* shape, double* area_out) {
    try {
        GProp_GProps props;
        BRepGProp::SurfaceProperties(*as_shape(shape), props);
        *area_out = props.Mass();
        return 0;
    } catch (Standard_Failure& e) {
        set_error(e.GetMessageString());
        return 1;
    } catch (...) {
        set_error("unknown C++ exception in occt_shape_surface_area");
        return 1;
    }
}

int occt_shape_centroid(void* shape, double* cx, double* cy, double* cz) {
    try {
        GProp_GProps props;
        BRepGProp::VolumeProperties(*as_shape(shape), props);
        gp_Pnt center = props.CentreOfMass();
        *cx = center.X();
        *cy = center.Y();
        *cz = center.Z();
        return 0;
    } catch (Standard_Failure& e) {
        set_error(e.GetMessageString());
        return 1;
    } catch (...) {
        set_error("unknown C++ exception in occt_shape_centroid");
        return 1;
    }
}

int occt_shape_face_count(void* shape, int* count_out) {
    try {
        int count = 0;
        for (TopExp_Explorer exp(*as_shape(shape), TopAbs_FACE); exp.More(); exp.Next()) {
            count++;
        }
        *count_out = count;
        return 0;
    } catch (Standard_Failure& e) {
        set_error(e.GetMessageString());
        return 1;
    } catch (...) {
        set_error("unknown C++ exception in occt_shape_face_count");
        return 1;
    }
}

int occt_shape_is_valid(void* shape, int* valid_out) {
    try {
        BRepCheck_Analyzer analyzer(*as_shape(shape));
        *valid_out = analyzer.IsValid() ? 1 : 0;
        return 0;
    } catch (Standard_Failure& e) {
        set_error(e.GetMessageString());
        return 1;
    } catch (...) {
        set_error("unknown C++ exception in occt_shape_is_valid");
        return 1;
    }
}

// ---- Primitives ----

int occt_make_box(double dx, double dy, double dz, void** shape_out) {
    try {
        BRepPrimAPI_MakeBox maker(dx, dy, dz);
        maker.Build();
        if (!maker.IsDone()) {
            set_error("BRepPrimAPI_MakeBox failed");
            return 1;
        }
        *shape_out = to_heap(maker.Shape());
        return 0;
    } catch (Standard_Failure& e) {
        set_error(e.GetMessageString());
        return 1;
    } catch (...) {
        set_error("unknown C++ exception in occt_make_box");
        return 1;
    }
}

int occt_make_cylinder(double radius, double height, void** shape_out) {
    try {
        BRepPrimAPI_MakeCylinder maker(radius, height);
        maker.Build();
        if (!maker.IsDone()) {
            set_error("BRepPrimAPI_MakeCylinder failed");
            return 1;
        }
        *shape_out = to_heap(maker.Shape());
        return 0;
    } catch (Standard_Failure& e) {
        set_error(e.GetMessageString());
        return 1;
    } catch (...) {
        set_error("unknown C++ exception in occt_make_cylinder");
        return 1;
    }
}

int occt_make_sphere(double radius, void** shape_out) {
    try {
        BRepPrimAPI_MakeSphere maker(radius);
        maker.Build();
        if (!maker.IsDone()) {
            set_error("BRepPrimAPI_MakeSphere failed");
            return 1;
        }
        *shape_out = to_heap(maker.Shape());
        return 0;
    } catch (Standard_Failure& e) {
        set_error(e.GetMessageString());
        return 1;
    } catch (...) {
        set_error("unknown C++ exception in occt_make_sphere");
        return 1;
    }
}

int occt_make_cone(double r1, double r2, double height, void** shape_out) {
    try {
        // OCCT MakeCone(R1, R2, H) — R2 can be 0 for a full cone
        double r2_clamped = (r2 < 1e-10) ? 0.0 : r2;
        BRepPrimAPI_MakeCone maker(r1, r2_clamped, height);
        maker.Build();
        if (!maker.IsDone()) {
            set_error("BRepPrimAPI_MakeCone failed");
            return 1;
        }
        *shape_out = to_heap(maker.Shape());
        return 0;
    } catch (Standard_Failure& e) {
        set_error(e.GetMessageString());
        return 1;
    } catch (...) {
        set_error("unknown C++ exception in occt_make_cone");
        return 1;
    }
}

// ---- Spatial predicates ----

int occt_shape_distance(void* shape1, void* shape2, double* dist_out) {
    try {
        BRepExtrema_DistShapeShape dist(*as_shape(shape1), *as_shape(shape2));
        if (!dist.IsDone()) {
            set_error("BRepExtrema_DistShapeShape failed");
            return 1;
        }
        *dist_out = dist.Value();
        return 0;
    } catch (Standard_Failure& e) {
        set_error(e.GetMessageString());
        return 1;
    } catch (...) {
        set_error("unknown C++ exception in occt_shape_distance");
        return 1;
    }
}

int occt_shape_intersects(void* shape1, void* shape2, int* result_out) {
    try {
        BRepExtrema_DistShapeShape dist(*as_shape(shape1), *as_shape(shape2));
        if (!dist.IsDone()) {
            set_error("BRepExtrema_DistShapeShape failed");
            return 1;
        }
        *result_out = (dist.Value() < 1e-7) ? 1 : 0;
        return 0;
    } catch (Standard_Failure& e) {
        set_error(e.GetMessageString());
        return 1;
    } catch (...) {
        set_error("unknown C++ exception in occt_shape_intersects");
        return 1;
    }
}

int occt_point_in_solid(void* shape, double x, double y, double z, int* result_out) {
    try {
        gp_Pnt point(x, y, z);
        BRepClass3d_SolidClassifier classifier(*as_shape(shape), point, 1e-7);
        TopAbs_State state = classifier.State();
        *result_out = (state == TopAbs_IN || state == TopAbs_ON) ? 1 : 0;
        return 0;
    } catch (Standard_Failure& e) {
        set_error(e.GetMessageString());
        return 1;
    } catch (...) {
        set_error("unknown C++ exception in occt_point_in_solid");
        return 1;
    }
}

// ---- Transforms ----

int occt_shape_translate(void* shape, double dx, double dy, double dz, void** out) {
    try {
        gp_Trsf trsf;
        trsf.SetTranslation(gp_Vec(dx, dy, dz));
        BRepBuilderAPI_Transform xform(*as_shape(shape), trsf, true);
        if (!xform.IsDone()) {
            set_error("BRepBuilderAPI_Transform (translate) failed");
            return 1;
        }
        *out = to_heap(xform.Shape());
        return 0;
    } catch (Standard_Failure& e) {
        set_error(e.GetMessageString());
        return 1;
    } catch (...) {
        set_error("unknown C++ exception in occt_shape_translate");
        return 1;
    }
}

int occt_shape_rotate(void* shape, double ax, double ay, double az,
                      double angle_rad, void** out) {
    try {
        gp_Ax1 axis(gp_Pnt(0, 0, 0), gp_Dir(ax, ay, az));
        gp_Trsf trsf;
        trsf.SetRotation(axis, angle_rad);
        BRepBuilderAPI_Transform xform(*as_shape(shape), trsf, true);
        if (!xform.IsDone()) {
            set_error("BRepBuilderAPI_Transform (rotate) failed");
            return 1;
        }
        *out = to_heap(xform.Shape());
        return 0;
    } catch (Standard_Failure& e) {
        set_error(e.GetMessageString());
        return 1;
    } catch (...) {
        set_error("unknown C++ exception in occt_shape_rotate");
        return 1;
    }
}

int occt_shape_transform(void* shape, const double* matrix, void** out) {
    try {
        gp_Trsf trsf;
        // matrix is 12 doubles: row-major [R11 R12 R13 tx | R21 R22 R23 ty | R31 R32 R33 tz]
        trsf.SetValues(
            matrix[0], matrix[1], matrix[2],  matrix[3],
            matrix[4], matrix[5], matrix[6],  matrix[7],
            matrix[8], matrix[9], matrix[10], matrix[11]
        );
        BRepBuilderAPI_Transform xform(*as_shape(shape), trsf, true);
        if (!xform.IsDone()) {
            set_error("BRepBuilderAPI_Transform (transform) failed");
            return 1;
        }
        *out = to_heap(xform.Shape());
        return 0;
    } catch (Standard_Failure& e) {
        set_error(e.GetMessageString());
        return 1;
    } catch (...) {
        set_error("unknown C++ exception in occt_shape_transform");
        return 1;
    }
}

// ---- Oriented Bounding Box ----

int occt_shape_obb(void* shape,
                   double* cx, double* cy, double* cz,
                   double* xx, double* xy, double* xz,
                   double* yx, double* yy, double* yz,
                   double* hx, double* hy, double* hz) {
    try {
        Bnd_OBB obb;
        BRepBndLib::AddOBB(*as_shape(shape), obb, true /* useTriangulation */, false, true);
        if (obb.IsVoid()) {
            set_error("OBB is void (empty shape)");
            return 1;
        }
        const gp_XYZ& center = obb.Center();
        *cx = center.X();
        *cy = center.Y();
        *cz = center.Z();

        const gp_XYZ& xdir = obb.XDirection();
        *xx = xdir.X();
        *xy = xdir.Y();
        *xz = xdir.Z();

        const gp_XYZ& ydir = obb.YDirection();
        *yx = ydir.X();
        *yy = ydir.Y();
        *yz = ydir.Z();

        *hx = obb.XHSize();
        *hy = obb.YHSize();
        *hz = obb.ZHSize();
        return 0;
    } catch (Standard_Failure& e) {
        set_error(e.GetMessageString());
        return 1;
    } catch (...) {
        set_error("unknown C++ exception in occt_shape_obb");
        return 1;
    }
}

// ---- Boolean operations ----

int occt_shape_fuse(void* shape1, void* shape2, void** out) {
    try {
        BRepAlgoAPI_Fuse fuser(*as_shape(shape1), *as_shape(shape2));
        if (fuser.HasErrors()) {
            set_error("BRepAlgoAPI_Fuse failed");
            return 1;
        }
        const TopoDS_Shape& result = fuser.Shape();
        if (result.IsNull()) {
            set_error("BRepAlgoAPI_Fuse produced null shape");
            return 1;
        }
        *out = to_heap(result);
        return 0;
    } catch (Standard_Failure& e) {
        set_error(e.GetMessageString());
        return 1;
    } catch (...) {
        set_error("unknown C++ exception in occt_shape_fuse");
        return 1;
    }
}

int occt_shape_cut(void* shape1, void* shape2, void** out) {
    try {
        BRepAlgoAPI_Cut cutter(*as_shape(shape1), *as_shape(shape2));
        if (cutter.HasErrors()) {
            set_error("BRepAlgoAPI_Cut failed");
            return 1;
        }
        const TopoDS_Shape& result = cutter.Shape();
        if (result.IsNull()) {
            set_error("BRepAlgoAPI_Cut produced null shape");
            return 1;
        }
        *out = to_heap(result);
        return 0;
    } catch (Standard_Failure& e) {
        set_error(e.GetMessageString());
        return 1;
    } catch (...) {
        set_error("unknown C++ exception in occt_shape_cut");
        return 1;
    }
}

int occt_shape_common(void* shape1, void* shape2, void** out) {
    try {
        BRepAlgoAPI_Common common(*as_shape(shape1), *as_shape(shape2));
        if (common.HasErrors()) {
            set_error("BRepAlgoAPI_Common failed");
            return 1;
        }
        const TopoDS_Shape& result = common.Shape();
        if (result.IsNull()) {
            set_error("BRepAlgoAPI_Common produced null shape");
            return 1;
        }
        *out = to_heap(result);
        return 0;
    } catch (Standard_Failure& e) {
        set_error(e.GetMessageString());
        return 1;
    } catch (...) {
        set_error("unknown C++ exception in occt_shape_common");
        return 1;
    }
}

// ---- Shape healing ----

int occt_shape_heal(void* shape, void** out) {
    try {
        Handle(ShapeFix_Shape) fixer = new ShapeFix_Shape(*as_shape(shape));
        fixer->Perform();
        TopoDS_Shape result = fixer->Shape();
        if (result.IsNull()) {
            set_error("ShapeFix_Shape produced null shape");
            return 1;
        }
        *out = to_heap(result);
        return 0;
    } catch (Standard_Failure& e) {
        set_error(e.GetMessageString());
        return 1;
    } catch (...) {
        set_error("unknown C++ exception in occt_shape_heal");
        return 1;
    }
}

// ---- STL export (manual binary STL) ----

int occt_shape_to_stl(void* shape, double linear_deflection, double angular_deflection,
                      uint8_t** data_out, size_t* len_out) {
    try {
        TopoDS_Shape& s = *as_shape(shape);

        // Tessellate shape
        BRepMesh_IncrementalMesh mesh(s, linear_deflection, Standard_False, angular_deflection);
        if (!mesh.IsDone()) {
            set_error("BRepMesh_IncrementalMesh failed");
            return 1;
        }

        // Count total triangles across all faces
        int total_triangles = 0;
        for (TopExp_Explorer exp(s, TopAbs_FACE); exp.More(); exp.Next()) {
            TopLoc_Location loc;
            Handle(Poly_Triangulation) tri = BRep_Tool::Triangulation(
                TopoDS::Face(exp.Current()), loc);
            if (!tri.IsNull()) {
                total_triangles += tri->NbTriangles();
            }
        }

        // Binary STL: 80-byte header + 4-byte count + 50 bytes per triangle
        size_t buf_size = 80 + 4 + (size_t)total_triangles * 50;
        uint8_t* buf = static_cast<uint8_t*>(malloc(buf_size));
        if (!buf) {
            set_error("malloc failed in occt_shape_to_stl");
            return 1;
        }
        memset(buf, 0, 80);  // Header (zeros)
        uint32_t tri_count_le = (uint32_t)total_triangles;
        memcpy(buf + 80, &tri_count_le, 4);

        size_t offset = 84;
        for (TopExp_Explorer exp(s, TopAbs_FACE); exp.More(); exp.Next()) {
            TopLoc_Location loc;
            Handle(Poly_Triangulation) tri = BRep_Tool::Triangulation(
                TopoDS::Face(exp.Current()), loc);
            if (tri.IsNull()) continue;

            const gp_Trsf& trsf = loc.Transformation();
            bool has_transform = !loc.IsIdentity();

            for (int i = 1; i <= tri->NbTriangles(); ++i) {
                const Poly_Triangle& triangle = tri->Triangle(i);
                int n1, n2, n3;
                triangle.Get(n1, n2, n3);

                gp_Pnt p1 = tri->Node(n1);
                gp_Pnt p2 = tri->Node(n2);
                gp_Pnt p3 = tri->Node(n3);

                if (has_transform) {
                    p1.Transform(trsf);
                    p2.Transform(trsf);
                    p3.Transform(trsf);
                }

                // Compute face normal from cross product
                gp_XYZ v1 = p2.XYZ() - p1.XYZ();
                gp_XYZ v2 = p3.XYZ() - p1.XYZ();
                gp_XYZ normal = v1.Crossed(v2);
                double mag = normal.Modulus();
                if (mag > 1e-10) {
                    normal.Divide(mag);
                }

                // Write 50-byte triangle record: normal(12) + v1(12) + v2(12) + v3(12) + attr(2)
                float coords[12];
                coords[0]  = (float)normal.X();
                coords[1]  = (float)normal.Y();
                coords[2]  = (float)normal.Z();
                coords[3]  = (float)p1.X();
                coords[4]  = (float)p1.Y();
                coords[5]  = (float)p1.Z();
                coords[6]  = (float)p2.X();
                coords[7]  = (float)p2.Y();
                coords[8]  = (float)p2.Z();
                coords[9]  = (float)p3.X();
                coords[10] = (float)p3.Y();
                coords[11] = (float)p3.Z();
                memcpy(buf + offset, coords, 48);
                offset += 48;
                // Attribute byte count (0)
                buf[offset] = 0;
                buf[offset + 1] = 0;
                offset += 2;
            }
        }

        *data_out = buf;
        *len_out = buf_size;
        return 0;
    } catch (Standard_Failure& e) {
        set_error(e.GetMessageString());
        return 1;
    } catch (...) {
        set_error("unknown C++ exception in occt_shape_to_stl");
        return 1;
    }
}

// ---- Extrusion (IFC-style linear sweep) ----

int occt_shape_extrude(double width, double height, double dx, double dy, double dz, void** out) {
    try {
        // Build a rectangular face on the XY plane at origin
        gp_Pnt p1(0, 0, 0), p2(width, 0, 0), p3(width, height, 0), p4(0, height, 0);
        TopoDS_Edge e1 = BRepBuilderAPI_MakeEdge(p1, p2).Edge();
        TopoDS_Edge e2 = BRepBuilderAPI_MakeEdge(p2, p3).Edge();
        TopoDS_Edge e3 = BRepBuilderAPI_MakeEdge(p3, p4).Edge();
        TopoDS_Edge e4 = BRepBuilderAPI_MakeEdge(p4, p1).Edge();
        TopoDS_Wire wire = BRepBuilderAPI_MakeWire(e1, e2, e3, e4).Wire();
        TopoDS_Face face = BRepBuilderAPI_MakeFace(wire).Face();

        gp_Vec direction(dx, dy, dz);
        BRepPrimAPI_MakePrism prism(face, direction);
        prism.Build();
        if (!prism.IsDone()) {
            set_error("BRepPrimAPI_MakePrism failed");
            return 1;
        }
        *out = to_heap(prism.Shape());
        return 0;
    } catch (Standard_Failure& e) {
        set_error(e.GetMessageString());
        return 1;
    } catch (...) {
        set_error("unknown C++ exception in occt_shape_extrude");
        return 1;
    }
}

// ---- STEP import/export ----

int occt_step_read(const uint8_t* data, size_t len, void** shape_out) {
    try {
        std::string step_str(reinterpret_cast<const char*>(data), len);
        std::istringstream iss(step_str);

        STEPControl_Reader reader;
        IFSelect_ReturnStatus status = reader.ReadStream("memory.step", iss);
        if (status != IFSelect_RetDone) {
            set_error("STEPControl_Reader::ReadStream failed (status " +
                      std::to_string(static_cast<int>(status)) + ")");
            return 1;
        }
        reader.TransferRoots();
        TopoDS_Shape shape = reader.OneShape();
        if (shape.IsNull()) {
            set_error("STEPControl_Reader produced null shape");
            return 1;
        }
        *shape_out = to_heap(shape);
        return 0;
    } catch (Standard_Failure& e) {
        set_error(e.GetMessageString());
        return 1;
    } catch (std::exception& e) {
        set_error(e.what());
        return 1;
    } catch (...) {
        set_error("unknown C++ exception in occt_step_read");
        return 1;
    }
}

int occt_step_write(void* shape, uint8_t** data_out, size_t* len_out) {
    try {
        STEPControl_Writer writer;
        IFSelect_ReturnStatus status = writer.Transfer(*as_shape(shape), STEPControl_AsIs);
        if (status != IFSelect_RetDone) {
            set_error("STEPControl_Writer::Transfer failed (status " +
                      std::to_string(static_cast<int>(status)) + ")");
            return 1;
        }
        std::ostringstream oss;
        status = writer.WriteStream(oss);
        if (status != IFSelect_RetDone) {
            set_error("STEPControl_Writer::WriteStream failed (status " +
                      std::to_string(static_cast<int>(status)) + ")");
            return 1;
        }
        std::string step_str = oss.str();
        *len_out = step_str.size();
        *data_out = static_cast<uint8_t*>(malloc(step_str.size()));
        if (!*data_out) {
            set_error("malloc failed in occt_step_write");
            return 1;
        }
        memcpy(*data_out, step_str.data(), step_str.size());
        return 0;
    } catch (Standard_Failure& e) {
        set_error(e.GetMessageString());
        return 1;
    } catch (std::exception& e) {
        set_error(e.what());
        return 1;
    } catch (...) {
        set_error("unknown C++ exception in occt_step_write");
        return 1;
    }
}

// ---- IGES import/export ----

int occt_iges_read(const uint8_t* data, size_t len, void** shape_out) {
    try {
        // IGESControl_Reader doesn't support ReadStream in OCCT 7.8,
        // so we write to a temp file and use ReadFile.
        char tmpname[] = "/tmp/pg_solid_iges_XXXXXX";
        int fd = mkstemp(tmpname);
        if (fd < 0) {
            set_error("mkstemp failed in occt_iges_read");
            return 1;
        }
        ssize_t written = ::write(fd, data, len);
        close(fd);
        if (written < 0 || static_cast<size_t>(written) != len) {
            unlink(tmpname);
            set_error("write to temp file failed in occt_iges_read");
            return 1;
        }

        IGESControl_Reader reader;
        IFSelect_ReturnStatus status = reader.ReadFile(tmpname);
        unlink(tmpname);
        if (status != IFSelect_RetDone) {
            set_error("IGESControl_Reader::ReadFile failed (status " +
                      std::to_string(static_cast<int>(status)) + ")");
            return 1;
        }
        reader.TransferRoots();
        TopoDS_Shape shape = reader.OneShape();
        if (shape.IsNull()) {
            set_error("IGESControl_Reader produced null shape");
            return 1;
        }
        *shape_out = to_heap(shape);
        return 0;
    } catch (Standard_Failure& e) {
        set_error(e.GetMessageString());
        return 1;
    } catch (std::exception& e) {
        set_error(e.what());
        return 1;
    } catch (...) {
        set_error("unknown C++ exception in occt_iges_read");
        return 1;
    }
}

int occt_iges_write(void* shape, uint8_t** data_out, size_t* len_out) {
    try {
        IGESControl_Writer writer;
        writer.AddShape(*as_shape(shape));
        writer.ComputeModel();
        std::ostringstream oss;
        Standard_Boolean ok = writer.Write(oss);
        if (!ok) {
            set_error("IGESControl_Writer::Write failed");
            return 1;
        }
        std::string iges_str = oss.str();
        *len_out = iges_str.size();
        *data_out = static_cast<uint8_t*>(malloc(iges_str.size()));
        if (!*data_out) {
            set_error("malloc failed in occt_iges_write");
            return 1;
        }
        memcpy(*data_out, iges_str.data(), iges_str.size());
        return 0;
    } catch (Standard_Failure& e) {
        set_error(e.GetMessageString());
        return 1;
    } catch (std::exception& e) {
        set_error(e.what());
        return 1;
    } catch (...) {
        set_error("unknown C++ exception in occt_iges_write");
        return 1;
    }
}

// ---- Mesh extraction (raw arrays for Rust serialization) ----

int occt_mesh_extract(void* shape, double linear_deflection, double angular_deflection,
                      float** positions_out, size_t* position_count,
                      float** normals_out, size_t* normal_count,
                      uint32_t** indices_out, size_t* index_count) {
    try {
        TopoDS_Shape& s = *as_shape(shape);

        // Tessellate shape
        BRepMesh_IncrementalMesh mesh(s, linear_deflection, Standard_False, angular_deflection);
        if (!mesh.IsDone()) {
            set_error("BRepMesh_IncrementalMesh failed");
            return 1;
        }

        // First pass: count total vertices and triangles
        size_t total_verts = 0;
        size_t total_tris = 0;
        for (TopExp_Explorer exp(s, TopAbs_FACE); exp.More(); exp.Next()) {
            TopLoc_Location loc;
            Handle(Poly_Triangulation) tri = BRep_Tool::Triangulation(
                TopoDS::Face(exp.Current()), loc);
            if (!tri.IsNull()) {
                total_verts += tri->NbNodes();
                total_tris += tri->NbTriangles();
            }
        }

        if (total_verts == 0) {
            set_error("shape has no tessellatable faces");
            return 1;
        }

        // Allocate arrays
        float* positions = static_cast<float*>(malloc(total_verts * 3 * sizeof(float)));
        float* normals = static_cast<float*>(malloc(total_verts * 3 * sizeof(float)));
        uint32_t* indices = static_cast<uint32_t*>(malloc(total_tris * 3 * sizeof(uint32_t)));
        if (!positions || !normals || !indices) {
            free(positions); free(normals); free(indices);
            set_error("malloc failed in occt_mesh_extract");
            return 1;
        }

        // Second pass: fill arrays
        size_t vert_offset = 0;
        size_t idx_offset = 0;
        for (TopExp_Explorer exp(s, TopAbs_FACE); exp.More(); exp.Next()) {
            TopoDS_Face face = TopoDS::Face(exp.Current());
            TopLoc_Location loc;
            Handle(Poly_Triangulation) tri = BRep_Tool::Triangulation(face, loc);
            if (tri.IsNull()) continue;

            const gp_Trsf& trsf = loc.Transformation();
            bool has_transform = !loc.IsIdentity();
            bool reversed = (face.Orientation() == TopAbs_REVERSED);

            // Ensure normals are available
            if (!tri->HasNormals()) {
                tri->ComputeNormals();
            }

            int nb_nodes = tri->NbNodes();

            // Extract vertices and normals
            for (int i = 1; i <= nb_nodes; ++i) {
                gp_Pnt p = tri->Node(i);
                if (has_transform) {
                    p.Transform(trsf);
                }
                positions[(vert_offset + i - 1) * 3 + 0] = (float)p.X();
                positions[(vert_offset + i - 1) * 3 + 1] = (float)p.Y();
                positions[(vert_offset + i - 1) * 3 + 2] = (float)p.Z();

                gp_Dir n = tri->Normal(i);
                if (has_transform) {
                    n.Transform(trsf);
                }
                if (reversed) {
                    n.Reverse();
                }
                normals[(vert_offset + i - 1) * 3 + 0] = (float)n.X();
                normals[(vert_offset + i - 1) * 3 + 1] = (float)n.Y();
                normals[(vert_offset + i - 1) * 3 + 2] = (float)n.Z();
            }

            // Extract triangle indices (offset by global vertex base)
            for (int i = 1; i <= tri->NbTriangles(); ++i) {
                int n1, n2, n3;
                tri->Triangle(i).Get(n1, n2, n3);
                if (reversed) {
                    std::swap(n2, n3);
                }
                indices[idx_offset * 3 + 0] = (uint32_t)(vert_offset + n1 - 1);
                indices[idx_offset * 3 + 1] = (uint32_t)(vert_offset + n2 - 1);
                indices[idx_offset * 3 + 2] = (uint32_t)(vert_offset + n3 - 1);
                idx_offset++;
            }

            vert_offset += nb_nodes;
        }

        *positions_out = positions;
        *position_count = total_verts * 3;
        *normals_out = normals;
        *normal_count = total_verts * 3;
        *indices_out = indices;
        *index_count = total_tris * 3;
        return 0;
    } catch (Standard_Failure& e) {
        set_error(e.GetMessageString());
        return 1;
    } catch (...) {
        set_error("unknown C++ exception in occt_mesh_extract");
        return 1;
    }
}

void occt_mesh_free(float* positions, float* normals, uint32_t* indices) {
    free(positions);
    free(normals);
    free(indices);
}

// ---- File-based import (reads directly from disk path) ----

int occt_step_read_file(const char* filepath, void** shape_out) {
    try {
        STEPControl_Reader reader;
        IFSelect_ReturnStatus status = reader.ReadFile(filepath);
        if (status != IFSelect_RetDone) {
            set_error("STEPControl_Reader::ReadFile failed (status " +
                      std::to_string(static_cast<int>(status)) + ")");
            return 1;
        }
        reader.TransferRoots();
        TopoDS_Shape shape = reader.OneShape();
        if (shape.IsNull()) {
            set_error("STEPControl_Reader produced null shape from file");
            return 1;
        }
        *shape_out = to_heap(shape);
        return 0;
    } catch (Standard_Failure& e) {
        set_error(e.GetMessageString());
        return 1;
    } catch (std::exception& e) {
        set_error(e.what());
        return 1;
    } catch (...) {
        set_error("unknown C++ exception in occt_step_read_file");
        return 1;
    }
}

int occt_iges_read_file(const char* filepath, void** shape_out) {
    try {
        IGESControl_Reader reader;
        IFSelect_ReturnStatus status = reader.ReadFile(filepath);
        if (status != IFSelect_RetDone) {
            set_error("IGESControl_Reader::ReadFile failed (status " +
                      std::to_string(static_cast<int>(status)) + ")");
            return 1;
        }
        reader.TransferRoots();
        TopoDS_Shape shape = reader.OneShape();
        if (shape.IsNull()) {
            set_error("IGESControl_Reader produced null shape from file");
            return 1;
        }
        *shape_out = to_heap(shape);
        return 0;
    } catch (Standard_Failure& e) {
        set_error(e.GetMessageString());
        return 1;
    } catch (std::exception& e) {
        set_error(e.what());
        return 1;
    } catch (...) {
        set_error("unknown C++ exception in occt_iges_read_file");
        return 1;
    }
}

// ---- Shape check (diagnostic) ----

int occt_shape_check(void* shape, char** msg_out, size_t* msg_len) {
    try {
        BRepCheck_Analyzer analyzer(*as_shape(shape));
        if (analyzer.IsValid()) {
            std::string result = "OK";
            *msg_len = result.size();
            *msg_out = static_cast<char*>(malloc(result.size() + 1));
            memcpy(*msg_out, result.c_str(), result.size() + 1);
            return 0;
        }

        int face_errors = 0, edge_errors = 0, vertex_errors = 0;
        TopExp_Explorer exp;

        for (exp.Init(*as_shape(shape), TopAbs_FACE); exp.More(); exp.Next()) {
            const Handle(BRepCheck_Result)& res = analyzer.Result(exp.Current());
            if (!res.IsNull()) {
                for (BRepCheck_ListOfStatus::Iterator it(res->Status()); it.More(); it.Next()) {
                    if (it.Value() != BRepCheck_NoError) face_errors++;
                }
            }
        }
        for (exp.Init(*as_shape(shape), TopAbs_EDGE); exp.More(); exp.Next()) {
            const Handle(BRepCheck_Result)& res = analyzer.Result(exp.Current());
            if (!res.IsNull()) {
                for (BRepCheck_ListOfStatus::Iterator it(res->Status()); it.More(); it.Next()) {
                    if (it.Value() != BRepCheck_NoError) edge_errors++;
                }
            }
        }
        for (exp.Init(*as_shape(shape), TopAbs_VERTEX); exp.More(); exp.Next()) {
            const Handle(BRepCheck_Result)& res = analyzer.Result(exp.Current());
            if (!res.IsNull()) {
                for (BRepCheck_ListOfStatus::Iterator it(res->Status()); it.More(); it.Next()) {
                    if (it.Value() != BRepCheck_NoError) vertex_errors++;
                }
            }
        }

        std::ostringstream oss;
        oss << "INVALID:";
        if (face_errors > 0) oss << " " << face_errors << " face error(s)";
        if (edge_errors > 0) oss << " " << edge_errors << " edge error(s)";
        if (vertex_errors > 0) oss << " " << vertex_errors << " vertex error(s)";

        std::string result = oss.str();
        *msg_len = result.size();
        *msg_out = static_cast<char*>(malloc(result.size() + 1));
        memcpy(*msg_out, result.c_str(), result.size() + 1);
        return 0;
    } catch (Standard_Failure& e) {
        set_error(e.GetMessageString());
        return 1;
    } catch (...) {
        set_error("unknown C++ exception in occt_shape_check");
        return 1;
    }
}

// ---- Offset shape (expand/shrink) ----

int occt_shape_offset(void* shape, double distance, void** out) {
    try {
        BRepOffsetAPI_MakeOffsetShape maker;
        maker.PerformByJoin(*as_shape(shape), distance, 1e-3);
        if (!maker.IsDone()) {
            set_error("BRepOffsetAPI_MakeOffsetShape failed");
            return 1;
        }
        const TopoDS_Shape& result = maker.Shape();
        if (result.IsNull()) {
            set_error("offset produced null shape");
            return 1;
        }
        *out = to_heap(result);
        return 0;
    } catch (Standard_Failure& e) {
        set_error(e.GetMessageString());
        return 1;
    } catch (...) {
        set_error("unknown C++ exception in occt_shape_offset");
        return 1;
    }
}

// ---- Section area (shared face area between two touching solids) ----

int occt_shape_section_area(void* shape1, void* shape2, double* area_out) {
    try {
        BRepAlgoAPI_Section section(*as_shape(shape1), *as_shape(shape2));
        section.Build();
        if (!section.IsDone()) {
            set_error("BRepAlgoAPI_Section failed");
            return 1;
        }
        const TopoDS_Shape& result = section.Shape();
        if (result.IsNull()) {
            *area_out = 0.0;
            return 0;
        }

        // Collect edges from the section result
        Handle(TopTools_HSequenceOfShape) edges = new TopTools_HSequenceOfShape();
        for (TopExp_Explorer exp(result, TopAbs_EDGE); exp.More(); exp.Next()) {
            edges->Append(exp.Current());
        }

        if (edges->IsEmpty()) {
            *area_out = 0.0;
            return 0;
        }

        // Connect edges into closed wires
        Handle(TopTools_HSequenceOfShape) wires = new TopTools_HSequenceOfShape();
        ShapeAnalysis_FreeBounds::ConnectEdgesToWires(edges, 1e-5, Standard_False, wires);

        // Build faces from wires and compute total area
        double total_area = 0.0;
        for (int i = 1; i <= wires->Length(); ++i) {
            TopoDS_Wire wire = TopoDS::Wire(wires->Value(i));
            try {
                BRepBuilderAPI_MakeFace face_maker(wire);
                if (face_maker.IsDone()) {
                    GProp_GProps props;
                    BRepGProp::SurfaceProperties(face_maker.Face(), props);
                    total_area += props.Mass();
                }
            } catch (...) {
                // Skip wires that can't form faces (non-planar, etc.)
                continue;
            }
        }

        *area_out = total_area;
        return 0;
    } catch (Standard_Failure& e) {
        set_error(e.GetMessageString());
        return 1;
    } catch (...) {
        set_error("unknown C++ exception in occt_shape_section_area");
        return 1;
    }
}

// ---- Slice: cross-section of shape with a plane ----

int occt_shape_slice(void* shape,
                     double ox, double oy, double oz,
                     double nx, double ny, double nz,
                     void** out) {
    try {
        gp_Pnt origin(ox, oy, oz);
        gp_Dir normal(nx, ny, nz);
        gp_Pln plane(origin, normal);

        BRepAlgoAPI_Section section(*as_shape(shape), plane);
        section.Build();
        if (!section.IsDone()) {
            set_error("BRepAlgoAPI_Section with plane failed");
            return 1;
        }
        const TopoDS_Shape& result = section.Shape();
        if (result.IsNull()) {
            set_error("slice produced empty result");
            return 1;
        }

        // Collect edges from the section result
        Handle(TopTools_HSequenceOfShape) edges = new TopTools_HSequenceOfShape();
        for (TopExp_Explorer exp(result, TopAbs_EDGE); exp.More(); exp.Next()) {
            edges->Append(exp.Current());
        }

        if (edges->IsEmpty()) {
            set_error("slice produced no edges");
            return 1;
        }

        // Connect edges into closed wires
        Handle(TopTools_HSequenceOfShape) wires = new TopTools_HSequenceOfShape();
        ShapeAnalysis_FreeBounds::ConnectEdgesToWires(edges, 1e-5, Standard_False, wires);

        // Build a compound of faces from the wires
        BRep_Builder builder;
        TopoDS_Compound compound;
        builder.MakeCompound(compound);
        int face_count = 0;

        for (int i = 1; i <= wires->Length(); ++i) {
            TopoDS_Wire wire = TopoDS::Wire(wires->Value(i));
            try {
                BRepBuilderAPI_MakeFace face_maker(plane, wire);
                if (face_maker.IsDone()) {
                    builder.Add(compound, face_maker.Face());
                    face_count++;
                }
            } catch (...) {
                continue;
            }
        }

        if (face_count == 0) {
            set_error("slice produced edges but no planar faces");
            return 1;
        }

        *out = new TopoDS_Shape(compound);
        return 0;
    } catch (Standard_Failure& e) {
        set_error(e.GetMessageString());
        return 1;
    } catch (...) {
        set_error("unknown C++ exception in occt_shape_slice");
        return 1;
    }
}

// ---- 2D projection: hidden line removal along a direction ----

int occt_shape_project_2d(void* shape,
                          double dx, double dy, double dz,
                          void** out) {
    try {
        // Set up HLR algorithm with the shape
        Handle(HLRBRep_Algo) hlr = new HLRBRep_Algo();
        hlr->Add(*as_shape(shape));

        // Create projector looking along direction (dx, dy, dz)
        gp_Ax2 axis(gp_Pnt(0, 0, 0), gp_Dir(dx, dy, dz));
        HLRAlgo_Projector projector(axis);
        hlr->Projector(projector);
        hlr->Update();
        hlr->Hide();

        // Extract visible edges
        HLRBRep_HLRToShape hlr_to_shape(hlr);

        BRep_Builder builder;
        TopoDS_Compound compound;
        builder.MakeCompound(compound);
        int edge_count = 0;

        // Add visible sharp edges
        TopoDS_Shape visible_sharp = hlr_to_shape.VCompound();
        if (!visible_sharp.IsNull()) {
            for (TopExp_Explorer exp(visible_sharp, TopAbs_EDGE); exp.More(); exp.Next()) {
                builder.Add(compound, exp.Current());
                edge_count++;
            }
        }

        // Add visible smooth edges
        TopoDS_Shape visible_smooth = hlr_to_shape.Rg1LineVCompound();
        if (!visible_smooth.IsNull()) {
            for (TopExp_Explorer exp(visible_smooth, TopAbs_EDGE); exp.More(); exp.Next()) {
                builder.Add(compound, exp.Current());
                edge_count++;
            }
        }

        // Add visible outlines
        TopoDS_Shape visible_outline = hlr_to_shape.OutLineVCompound();
        if (!visible_outline.IsNull()) {
            for (TopExp_Explorer exp(visible_outline, TopAbs_EDGE); exp.More(); exp.Next()) {
                builder.Add(compound, exp.Current());
                edge_count++;
            }
        }

        if (edge_count == 0) {
            set_error("2D projection produced no visible edges");
            return 1;
        }

        *out = new TopoDS_Shape(compound);
        return 0;
    } catch (Standard_Failure& e) {
        set_error(e.GetMessageString());
        return 1;
    } catch (...) {
        set_error("unknown C++ exception in occt_shape_project_2d");
        return 1;
    }
}

// ---- Compound explode: count and extract solid children ----

int occt_shape_solid_count(void* shape, int* count_out) {
    try {
        int count = 0;
        for (TopExp_Explorer exp(*as_shape(shape), TopAbs_SOLID); exp.More(); exp.Next()) {
            count++;
        }
        *count_out = count;
        return 0;
    } catch (Standard_Failure& e) {
        set_error(e.GetMessageString());
        return 1;
    } catch (...) {
        set_error("unknown C++ exception in occt_shape_solid_count");
        return 1;
    }
}

int occt_shape_solid_at(void* shape, int index, void** out) {
    try {
        int i = 0;
        for (TopExp_Explorer exp(*as_shape(shape), TopAbs_SOLID); exp.More(); exp.Next()) {
            if (i == index) {
                *out = new TopoDS_Shape(TopoDS::Solid(exp.Current()));
                return 0;
            }
            i++;
        }
        set_error("solid index out of range");
        return 1;
    } catch (Standard_Failure& e) {
        set_error(e.GetMessageString());
        return 1;
    } catch (...) {
        set_error("unknown C++ exception in occt_shape_solid_at");
        return 1;
    }
}

// ---- IFC geometry construction functions ----

// Create a rectangular face centered at origin on XY plane.
int occt_make_rectangle_face(double width, double height, void** face_out) {
    try {
        double hw = width / 2.0;
        double hh = height / 2.0;
        gp_Pnt p1(-hw, -hh, 0), p2(hw, -hh, 0), p3(hw, hh, 0), p4(-hw, hh, 0);
        TopoDS_Edge e1 = BRepBuilderAPI_MakeEdge(p1, p2).Edge();
        TopoDS_Edge e2 = BRepBuilderAPI_MakeEdge(p2, p3).Edge();
        TopoDS_Edge e3 = BRepBuilderAPI_MakeEdge(p3, p4).Edge();
        TopoDS_Edge e4 = BRepBuilderAPI_MakeEdge(p4, p1).Edge();
        TopoDS_Wire wire = BRepBuilderAPI_MakeWire(e1, e2, e3, e4).Wire();
        TopoDS_Face face = BRepBuilderAPI_MakeFace(wire).Face();
        *face_out = to_heap(face);
        return 0;
    } catch (Standard_Failure& e) {
        set_error(e.GetMessageString());
        return 1;
    } catch (...) {
        set_error("unknown C++ exception in occt_make_rectangle_face");
        return 1;
    }
}

// Create a circular face centered at origin on XY plane.
int occt_make_circle_face(double radius, void** face_out) {
    try {
        gp_Ax2 axis(gp_Pnt(0, 0, 0), gp_Dir(0, 0, 1));
        Handle(Geom_Circle) circle = new Geom_Circle(axis, radius);
        TopoDS_Edge edge = BRepBuilderAPI_MakeEdge(circle).Edge();
        TopoDS_Wire wire = BRepBuilderAPI_MakeWire(edge).Wire();
        TopoDS_Face face = BRepBuilderAPI_MakeFace(wire).Face();
        *face_out = to_heap(face);
        return 0;
    } catch (Standard_Failure& e) {
        set_error(e.GetMessageString());
        return 1;
    } catch (...) {
        set_error("unknown C++ exception in occt_make_circle_face");
        return 1;
    }
}

// Create a face from a closed polygon (3D points, flat array: x0,y0,z0,x1,y1,z1,...).
int occt_make_polygon_face(const double* points, int npoints, void** face_out) {
    try {
        if (npoints < 3) {
            set_error("polygon needs at least 3 points");
            return 1;
        }
        BRepBuilderAPI_MakeWire wire_maker;
        for (int i = 0; i < npoints; ++i) {
            int j = (i + 1) % npoints;
            gp_Pnt p1(points[i*3], points[i*3+1], points[i*3+2]);
            gp_Pnt p2(points[j*3], points[j*3+1], points[j*3+2]);
            if (p1.Distance(p2) < 1e-10) continue;
            TopoDS_Edge edge = BRepBuilderAPI_MakeEdge(p1, p2).Edge();
            wire_maker.Add(edge);
        }
        if (!wire_maker.IsDone()) {
            set_error("BRepBuilderAPI_MakeWire failed for polygon");
            return 1;
        }
        TopoDS_Face face = BRepBuilderAPI_MakeFace(wire_maker.Wire()).Face();
        *face_out = to_heap(face);
        return 0;
    } catch (Standard_Failure& e) {
        set_error(e.GetMessageString());
        return 1;
    } catch (...) {
        set_error("unknown C++ exception in occt_make_polygon_face");
        return 1;
    }
}

// Extrude a face along a direction vector to create a solid.
int occt_extrude_face(void* face, double dx, double dy, double dz, void** out) {
    try {
        gp_Vec direction(dx, dy, dz);
        BRepPrimAPI_MakePrism prism(*as_shape(face), direction);
        prism.Build();
        if (!prism.IsDone()) {
            set_error("BRepPrimAPI_MakePrism failed");
            return 1;
        }
        *out = to_heap(prism.Shape());
        return 0;
    } catch (Standard_Failure& e) {
        set_error(e.GetMessageString());
        return 1;
    } catch (...) {
        set_error("unknown C++ exception in occt_extrude_face");
        return 1;
    }
}

// Build a solid from faceted boundary representation (polygon faces).
// all_points: flat [x,y,z,...] for all vertices of all faces
// face_starts: index (in points, not doubles) where each face begins
// face_sizes: number of vertices per face
int occt_make_faceted_solid(
    const double* all_points,
    const int* face_starts,
    const int* face_sizes,
    int nfaces,
    void** out
) {
    try {
        BRepBuilderAPI_Sewing sewer(1e-6);

        for (int f = 0; f < nfaces; ++f) {
            int start = face_starts[f];
            int npts = face_sizes[f];
            if (npts < 3) continue;

            BRepBuilderAPI_MakeWire wire_maker;
            int valid_edges = 0;
            for (int i = 0; i < npts; ++i) {
                int j = (i + 1) % npts;
                int pi = (start + i) * 3;
                int pj = (start + j) * 3;
                gp_Pnt p1(all_points[pi], all_points[pi+1], all_points[pi+2]);
                gp_Pnt p2(all_points[pj], all_points[pj+1], all_points[pj+2]);
                if (p1.Distance(p2) < 1e-10) continue;
                TopoDS_Edge edge = BRepBuilderAPI_MakeEdge(p1, p2).Edge();
                wire_maker.Add(edge);
                valid_edges++;
            }
            if (valid_edges < 3 || !wire_maker.IsDone()) continue;

            BRepBuilderAPI_MakeFace face_maker(wire_maker.Wire());
            if (face_maker.IsDone()) {
                sewer.Add(face_maker.Face());
            }
        }

        sewer.Perform();
        TopoDS_Shape sewn = sewer.SewedShape();

        if (sewn.IsNull()) {
            set_error("faceted solid sewing produced null shape");
            return 1;
        }

        // Try to make a solid from the sewn shell
        if (sewn.ShapeType() == TopAbs_SHELL) {
            BRep_Builder builder;
            TopoDS_Solid solid;
            builder.MakeSolid(solid);
            builder.Add(solid, sewn);
            Handle(ShapeFix_Shape) fixer = new ShapeFix_Shape(solid);
            fixer->Perform();
            *out = to_heap(fixer->Shape());
        } else {
            *out = to_heap(sewn);
        }
        return 0;
    } catch (Standard_Failure& e) {
        set_error(e.GetMessageString());
        return 1;
    } catch (...) {
        set_error("unknown C++ exception in occt_make_faceted_solid");
        return 1;
    }
}

// Assemble multiple shapes into a compound.
int occt_make_compound(void** shapes, int nshapes, void** out) {
    try {
        BRep_Builder builder;
        TopoDS_Compound compound;
        builder.MakeCompound(compound);
        for (int i = 0; i < nshapes; ++i) {
            if (shapes[i] != nullptr) {
                builder.Add(compound, *as_shape(shapes[i]));
            }
        }
        *out = to_heap(compound);
        return 0;
    } catch (Standard_Failure& e) {
        set_error(e.GetMessageString());
        return 1;
    } catch (...) {
        set_error("unknown C++ exception in occt_make_compound");
        return 1;
    }
}

} // extern "C"
