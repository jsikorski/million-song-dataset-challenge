function(songId, plays) {
    var playsMap = {}
    for (var i = 0; i < plays.length; i++) {
        var play = plays[i];
        var songIndex = play.song_index;

        if (playsMap[songIndex] == undefined) {
            playsMap[songIndex] = 0;
        }

        playsMap[songIndex] += play.play_count;
    }
    return playsMap;
}