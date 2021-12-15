function sleep(duration) {
    var now = new Date().getTime();
    while (new Date().getTime() < now + duration) {}
}

function foo() {}

sleep(2);
