import pystache

def render_to_file(src, target, ctx):
    renderer = pystache.Renderer()
    with open(target, 'w') as f:
        f.write(renderer.render_path(src, ctx))