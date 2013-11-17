function() {
    emit(this.user_index, { plays: [{ song_index: this.song_index, play_count: this.play_count }] });
}