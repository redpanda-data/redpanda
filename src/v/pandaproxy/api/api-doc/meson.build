pandaproxy_api_h = []
foreach swag : ['rest', 'schema_registry']
  t = custom_target(
    output: f'@swag@.json.h',
    input: f'@swag@.json',
    command: [seastar_json2code, '-f', '@INPUT@', '-o', '@OUTPUT@'])
  pandaproxy_api_h += [t]
endforeach
