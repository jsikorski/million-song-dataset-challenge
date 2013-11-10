function(songId, playCounts) {
    var totalPlayCount = 0;
    for (var i = 0; i < playCounts.length; i++) {
        totalPlayCount += playCounts[i];
    }
    return totalPlayCount;
}