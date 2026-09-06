// Sort existing display values only; never derive or overwrite signal metrics.
function createTableSorter(selector, columns, onChange) {
  const table = document.querySelector(selector);
  const collator = new Intl.Collator('tr', {numeric: true, sensitivity: 'base'});
  let activeColumn = null;
  let ascending = true;
  const headers = Array.from(table.tHead.rows[0].cells);
  const missing = value => value === null || value === undefined || value === '' || (typeof value === 'number' && !Number.isFinite(value));
  columns.forEach((column, index) => {
    if (!column) return;
    const header = headers[index];
    const label = header.textContent.trim();
    const button = document.createElement('button');
    button.type = 'button';
    button.className = 'table-sort-button';
    button.append(document.createTextNode(label));
    const arrow = document.createElement('span');
    arrow.className = 'table-sort-arrow';
    arrow.setAttribute('aria-hidden', 'true');
    arrow.textContent = '↕';
    button.append(arrow);
    header.replaceChildren(button);
    header.classList.add('sortable-heading');
    header.setAttribute('aria-sort', 'none');
    header.title = column.hint || `${label} sıralaması`;
    header.addEventListener('click', () => {
      ascending = activeColumn === index ? !ascending : true;
      activeColumn = index;
      headers.forEach((cell, position) => {
        if (!columns[position]) return;
        cell.setAttribute('aria-sort', position === index ? (ascending ? 'ascending' : 'descending') : 'none');
        cell.querySelector('.table-sort-arrow').textContent = position === index ? (ascending ? '↑' : '↓') : '↕';
      });
      onChange();
    });
  });
  return {
    sort(rows) {
      if (activeColumn === null) return rows;
      const column = columns[activeColumn];
      return [...rows].sort((left, right) => {
        const a = column.value(left);
        const b = column.value(right);
        if (missing(a)) return missing(b) ? 0 : 1;
        if (missing(b)) return -1;
        const comparison = column.numeric ? Number(a) - Number(b) : collator.compare(String(a), String(b));
        return ascending ? comparison : -comparison;
      });
    },
  };
}
