function to_upper(obj) {
    let record = obj.consume();
    let raw = record.get('value')
    let old_value = new Int8Array(raw)
    let new_value = new Int8Array(old_value.length)
    for (let i = 0; i < old_value.length; i++) {
        if (old_value[i] >= 97 && old_value[i] <= 122) {
            new_value[i] = old_value[i] - 32
        } else {
            new_value[i] = old_value[i]
        }
    }
    new_map = new Map(record)
    new_map.set('value', new_value.buffer)
    obj.produce(new_map)
}
