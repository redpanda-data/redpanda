admin_api_swags = [
  'config',
  'cluster_config',
  'raft',
  'security',
  'status',
  'features',
  'broker',
  'partition',
  'hbadger',
  'transaction',
  'cluster',
  'debug',
  'shadow_indexing',
  'redpanda_services_restart']

admin_api_h = []
foreach swag : admin_api_swags
  t = custom_target(
    output: f'@swag@.json.h',
    input: f'@swag@.json',
    command: [seastar_json2code, '-f', '@INPUT@', '-o', '@OUTPUT@'])
  admin_api_h += [t]
endforeach
