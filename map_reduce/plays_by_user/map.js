function() {
    emit(this.user_index, { song_index: this.song_index, play_count: this.play_count });
}