function(songId, values) {
    var userIds = [];
    values.forEach(function(value) {
        Array.prototype.push.apply(userIds, value.userIds);
    });

    return { userIds: userIds };
}