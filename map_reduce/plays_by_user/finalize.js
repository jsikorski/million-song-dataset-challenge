function(key, reducedValue) {
    var plays = reducedValue.plays;
    var playsMap = {};

    for (var i = 0; i < plays.length; i++) {
        var play = plays[i];
        var songIndex = play.song_index;
        var playCount = play.play_count;

        if (playsMap[songIndex] === undefined) {
            playsMap[songIndex] = playCount;
        }
        else {
            playsMap[songIndex] += playCount;
        }
    }

    return playsMap;
}