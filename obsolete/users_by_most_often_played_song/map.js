function() {
    var mostOftenPlayedSongId;
    var maxNumberOfPlays = 0;

    var plays = this.value;
    for (var songId in plays) {
        if (plays.hasOwnProperty(songId)) {
            var numberOfPlays = plays[songId];

            if (numberOfPlays > maxNumberOfPlays) {
                mostOftenPlayedSongId = songId;
                maxNumberOfPlays = numberOfPlays;
            }
        }
    }

    var userId = this._id;
    emit(mostOftenPlayedSongId, { userIds: [ userId ] });
}