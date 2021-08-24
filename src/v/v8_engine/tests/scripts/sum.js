function sum(obj) {
    let record = obj.consume();
    let raw = record.get('value')
    let old_value = new Int32Array(raw)
    old_value[2] = old_value[0] + old_value[1];

    new_map = new Map(record)
    new_map.set('value', old_value.buffer)
    obj.produce(new_map)
}
