fs = import('fs')
go = find_program('go')
go_build = find_program('build.sh')

kafka_request_generator = custom_target(
  input: [
    fs.copyfile('kafka-request-generator.go'),
    fs.copyfile('go.mod'),
    fs.copyfile('go.sum'),
  ],
  output: 'kafka-request-generator',
  command: [go_build, go, '@OUTPUT@'])
