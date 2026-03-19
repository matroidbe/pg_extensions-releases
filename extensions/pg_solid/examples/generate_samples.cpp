// Generate sample STEP and IGES files for testing pg_solid import.
// Build: g++ -std=c++17 -o generate_samples generate_samples.cpp \
//   -I/usr/include/opencascade \
//   -lTKernel -lTKMath -lTKBRep -lTKPrim -lTKTopAlgo -lTKDESTEP -lTKDEIGES \
//   -lTKGeomBase -lTKG3d -lTKG2d -lTKShHealing -lTKBool -lTKBO -lTKGeomAlgo
// Run: ./generate_samples

#include <iostream>
#include <BRepPrimAPI_MakeBox.hxx>
#include <BRepPrimAPI_MakeCylinder.hxx>
#include <BRepPrimAPI_MakeSphere.hxx>
#include <BRepAlgoAPI_Cut.hxx>
#include <BRepAlgoAPI_Fuse.hxx>
#include <BRepBuilderAPI_Transform.hxx>
#include <gp_Trsf.hxx>
#include <gp_Vec.hxx>
#include <STEPControl_Writer.hxx>
#include <IGESControl_Writer.hxx>
#include <IFSelect_ReturnStatus.hxx>

static void write_step(const TopoDS_Shape& shape, const char* filename) {
    STEPControl_Writer writer;
    writer.Transfer(shape, STEPControl_AsIs);
    if (writer.Write(filename) != IFSelect_RetDone) {
        std::cerr << "Failed to write " << filename << std::endl;
    } else {
        std::cout << "Wrote " << filename << std::endl;
    }
}

static void write_iges(const TopoDS_Shape& shape, const char* filename) {
    IGESControl_Writer writer;
    writer.AddShape(shape);
    writer.ComputeModel();
    if (!writer.Write(filename)) {
        std::cerr << "Failed to write " << filename << std::endl;
    } else {
        std::cout << "Wrote " << filename << std::endl;
    }
}

int main() {
    // 1. Simple box (building footprint: 10m x 8m x 3m)
    TopoDS_Shape box = BRepPrimAPI_MakeBox(10000, 8000, 3000).Shape();
    write_step(box, "box_10x8x3m.step");
    write_iges(box, "box_10x8x3m.iges");

    // 2. Cylinder (column: radius 300mm, height 3m)
    TopoDS_Shape cyl = BRepPrimAPI_MakeCylinder(300, 3000).Shape();
    write_step(cyl, "column_r300_h3000.step");
    write_iges(cyl, "column_r300_h3000.iges");

    // 3. Box with hole (wall with window opening)
    TopoDS_Shape wall = BRepPrimAPI_MakeBox(5000, 200, 3000).Shape();
    // Window: 1200x1500 at position (1500, -100, 800)
    TopoDS_Shape window = BRepPrimAPI_MakeBox(1200, 400, 1500).Shape();
    gp_Trsf trsf;
    trsf.SetTranslation(gp_Vec(1500, -100, 800));
    BRepBuilderAPI_Transform xform(window, trsf, true);
    TopoDS_Shape wall_with_window = BRepAlgoAPI_Cut(wall, xform.Shape()).Shape();
    write_step(wall_with_window, "wall_with_window.step");
    write_iges(wall_with_window, "wall_with_window.iges");

    // 4. Two-room building (two fused boxes)
    TopoDS_Shape room1 = BRepPrimAPI_MakeBox(6000, 5000, 3000).Shape();
    TopoDS_Shape room2_raw = BRepPrimAPI_MakeBox(4000, 5000, 3000).Shape();
    gp_Trsf t2;
    t2.SetTranslation(gp_Vec(6000, 0, 0));
    BRepBuilderAPI_Transform xf2(room2_raw, t2, true);
    TopoDS_Shape building = BRepAlgoAPI_Fuse(room1, xf2.Shape()).Shape();
    write_step(building, "two_room_building.step");
    write_iges(building, "two_room_building.iges");

    // 5. Sphere (test non-planar geometry)
    TopoDS_Shape sphere = BRepPrimAPI_MakeSphere(500).Shape();
    write_step(sphere, "sphere_r500.step");
    write_iges(sphere, "sphere_r500.iges");

    std::cout << "Done. Generated 10 sample files." << std::endl;
    return 0;
}
