let
  pkgs = import <nixpkgs> {};
in pkgs.mkShell {
  packages = [
    pkgs.uv
    pkgs.python312Full
  ];

  env = {
    UV_LINK_MODE = "copy"; 
    
    UV_PYTHON = "${pkgs.python312Full}/bin/python3"; 
    
    LD_LIBRARY_PATH = pkgs.lib.makeLibraryPath [
      pkgs.stdenv.cc.cc.lib
      pkgs.zlib
    ];
  };
}
