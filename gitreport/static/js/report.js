function toggleFiles(btn) {
  const list = btn.nextElementSibling;
  list.classList.toggle('open');
  btn.textContent = list.classList.contains('open') ? '▼ hide files' : '▶ ' + btn.dataset.count + ' files changed';
}
