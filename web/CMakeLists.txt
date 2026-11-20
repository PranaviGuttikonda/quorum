# Given: the visualizer is the engine compiled to WebAssembly plus a static
#        page.
# Approach: `quorum_web` links the embind bindings into quorum.js and
#           quorum.wasm (an ES module factory, createQuorum) and writes them,
#           with the page files, to ${CMAKE_BINARY_DIR}/site. That directory
#           is what GitHub Pages serves. ENVIRONMENT includes node, so the
#           determinism check can load the same module.
#   emcmake cmake -S . -B build-web -DCMAKE_BUILD_TYPE=Release
#   cmake --build build-web --target quorum_web
set(QUORUM_SITE ${CMAKE_BINARY_DIR}/site)

add_executable(quorum_web bindings.cpp)
target_link_libraries(quorum_web PRIVATE quorum_websim quorum_warnings)
target_link_options(quorum_web PRIVATE
  -lembind
  -sMODULARIZE=1 -sEXPORT_ES6=1 -sEXPORT_NAME=createQuorum
  -sENVIRONMENT=web,node
  -sALLOW_MEMORY_GROWTH=1
  -sFILESYSTEM=0
)
set_target_properties(quorum_web PROPERTIES
  OUTPUT_NAME quorum
  SUFFIX ".js"
  RUNTIME_OUTPUT_DIRECTORY ${QUORUM_SITE})

set(QUORUM_PAGE_FILES index.html style.css app.js og-image.png
  fonts/IBMPlexSans-Regular-Latin1.woff2 fonts/IBMPlexSans-SemiBold-Latin1.woff2 fonts/OFL.txt)
foreach(f ${QUORUM_PAGE_FILES})
  configure_file(${f} ${QUORUM_SITE}/${f} COPYONLY)
endforeach()
