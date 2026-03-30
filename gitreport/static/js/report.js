function toggleFiles(btn) {
  const list = btn.nextElementSibling;
  list.classList.toggle('open');
  btn.textContent = list.classList.contains('open') ? '▼ hide files' : '▶ ' + btn.dataset.count + ' files changed';
}

function toggleMorePRs(btn) {
  const list = btn.closest('.pr-list');
  const hidden = list.querySelectorAll('.pr-card-overflow');
  const showing = hidden[0] && hidden[0].style.display !== 'none';
  hidden.forEach(function(el) { el.style.display = showing ? 'none' : ''; });
  var count = hidden.length;
  btn.textContent = showing ? 'Show ' + count + ' more…' : 'Show fewer';
}
