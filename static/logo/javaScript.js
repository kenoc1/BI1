function unlockSprintButton() {
    let unlockBox = document.getElementById('unlockSprintBox')
    document.getElementById('submitSprint').disabled = unlockBox.checked;
}