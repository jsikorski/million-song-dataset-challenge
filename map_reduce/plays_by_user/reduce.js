function(userIndex, values) {
    plays = [];
    values.forEach(function(value) {
        Array.prototype.push.apply(plays, value.plays);
    });

    return { plays: plays };
}