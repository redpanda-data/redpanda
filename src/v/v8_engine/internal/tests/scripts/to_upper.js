function to_upper(obj) {
    let array = new Int8Array(obj);
    for (let i = 0; i < obj.byteLength; i++) {
        if (array[i] >= 97 && array[i] <= 122) {
                array[i] = array[i] - 32;
        } else {
            array[i] = array[i];
        }
    }
}
