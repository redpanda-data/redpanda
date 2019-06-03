#!/usr/bin/env python3

def render_to_file(src, target, ctx):
    import pystache
    renderer = pystache.Renderer()
    with open(target, 'w') as f:
        f.write(renderer.render_path(src, ctx))
