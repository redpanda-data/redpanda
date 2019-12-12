import jinja2

def load_from_file(path):
    with open(path, 'r') as f:
        return f.read()


def render_to_file(src, target, ctx):
    loader = jinja2.FunctionLoader(load_from_file)
    env = jinja2.Environment(loader=loader)
    with open(target, 'w') as f:
        s = env.get_template(src).render(ctx)
        f.write(env.get_template(src).render(ctx))
