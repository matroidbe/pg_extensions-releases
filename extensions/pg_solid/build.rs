fn main() {
    // Compile the C++ OCCT wrapper
    cc::Build::new()
        .cpp(true)
        .file("src/cpp/occt_wrapper.cpp")
        .flag("-std=c++17")
        .include("/usr/include/opencascade")
        .warnings(false) // OCCT headers produce many warnings
        .compile("occt_wrapper");

    // Link OCCT shared libraries (system packages)
    let occt_libs = [
        "TKernel",     // Foundation: handles, memory, math primitives
        "TKMath",      // 3D geometry primitives: gp_Pnt, gp_Vec
        "TKBRep",      // B-Rep data structures: TopoDS_Shape
        "TKGeomBase",  // Geometric curves and surfaces
        "TKG3d",       // 3D geometry
        "TKG2d",       // 2D geometry
        "TKTopAlgo",   // Topological algorithms, BRepExtrema
        "TKPrim",      // Primitive constructors: MakeBox, MakeCylinder
        "TKShHealing", // Shape healing: ShapeFix_Shape
        "TKGeomAlgo",  // Geometric algorithms (required by TKBO)
        "TKBO",        // Boolean operation kernel
        "TKBool",      // BRepAlgoAPI_Fuse/Cut/Common
        "TKMesh",      // BRepMesh_IncrementalMesh for STL tessellation
        "TKOffset",    // Offset/shell operations (BRepOffsetAPI_MakeOffsetShape)
        "TKHLR",       // Hidden Line Removal for 2D projection
        "TKDESTEP",    // STEP file import/export (STEPControl_Reader/Writer)
        "TKDEIGES",    // IGES file import/export (IGESControl_Reader/Writer)
    ];

    for lib in &occt_libs {
        println!("cargo:rustc-link-lib=dylib={lib}");
    }

    // Link C++ standard library
    println!("cargo:rustc-link-lib=dylib=stdc++");

    // Rebuild if C++ source changes
    println!("cargo:rerun-if-changed=src/cpp/occt_wrapper.cpp");
}
