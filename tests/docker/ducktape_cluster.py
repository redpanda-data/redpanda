import jinja2
import sys
import os

# A script to render ducktape_cluster.json from
# a jinja2 template and a CLI parameter specifying the number
# of nodes (i.e. the --scale parameter to docker-compose)

scale = int(sys.argv[1])
template_file = os.path.join(os.path.dirname(__file__),
                             'ducktape_cluster.json.j2')
template = jinja2.Template(open(template_file).read())
out_file = os.path.join(os.path.dirname(__file__), 'ducktape_cluster.json')

print(f"Writing ducktape config for {scale} nodes to {out_file}")
open(out_file, 'w').write(template.render(scale=scale))
