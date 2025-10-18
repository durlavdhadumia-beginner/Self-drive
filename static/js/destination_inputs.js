(() => {
    const initDestinationInputs = (container) => {
        const inputName = container.dataset.inputName || 'destinations';
        const datalistId = container.dataset.datalist || '';
        const addButtonSelector = container.dataset.addButton || '';
        const required = container.dataset.required === 'true';
        let initial = [];
        try {
            initial = JSON.parse(container.dataset.initial || '[]');
        } catch (error) {
            initial = [];
        }
        const addButton = addButtonSelector ? document.querySelector(addButtonSelector) : null;

        const getRows = () => Array.from(container.querySelectorAll('[data-role="destination-input-row"]'));

        const updateRemoveButtons = () => {
            const rows = getRows();
            rows.forEach((row, index) => {
                const removeBtn = row.querySelector('[data-role="remove-destination"]');
                if (removeBtn) {
                    removeBtn.disabled = required && rows.length === 1 && index === 0;
                }
                const input = row.querySelector('input');
                if (input) {
                    input.required = !!(required && index === 0);
                }
            });
        };

        const bindRow = (row, isFirst = false) => {
            if (!row) {
                return;
            }
            row.classList.add('destination-input-row');
            row.dataset.role = 'destination-input-row';
            let input = row.querySelector('input');
            if (!input) {
                input = document.createElement('input');
                input.type = 'text';
                row.insertBefore(input, row.firstChild || null);
            }
            input.name = inputName;
            input.classList.add('form-control');
            input.placeholder = input.placeholder || 'Enter destination';
            input.autocomplete = 'off';
            if (datalistId) {
                input.setAttribute('list', datalistId);
            } else {
                input.removeAttribute('list');
            }
            input.required = !!(required && isFirst);

            let removeBtn = row.querySelector('[data-role="remove-destination"]');
            if (!removeBtn) {
                removeBtn = document.createElement('button');
                removeBtn.type = 'button';
                removeBtn.className = 'btn btn-outline-danger';
                removeBtn.dataset.role = 'remove-destination';
                removeBtn.innerHTML = '<i class="bi bi-x-lg"></i>';
                row.appendChild(removeBtn);
            }
            if (!removeBtn.dataset.bound) {
                removeBtn.addEventListener('click', () => {
                    const rows = getRows();
                    if (required && rows.length === 1) {
                        input.value = '';
                        input.focus();
                        return;
                    }
                    row.remove();
                    updateRemoveButtons();
                });
                removeBtn.dataset.bound = '1';
            }
        };

        const createRow = (value = '', isFirst = false) => {
            const row = document.createElement('div');
            row.className = 'input-group destination-input-row';
            const input = document.createElement('input');
            input.type = 'text';
            if (value) {
                input.value = value;
            }
            row.appendChild(input);
            bindRow(row, isFirst);
            return row;
        };

        const addRow = (value = '', focus = true) => {
            const isFirst = getRows().length === 0;
            const row = createRow(value, isFirst);
            container.appendChild(row);
            updateRemoveButtons();
            if (focus) {
                const input = row.querySelector('input');
                if (input) {
                    input.focus();
                }
            }
        };

        const existingRows = getRows();
        if (existingRows.length) {
            existingRows.forEach((row, index) => bindRow(row, index === 0));
            updateRemoveButtons();
        } else if (initial.length) {
            initial.forEach((value, index) => addRow(value, index === initial.length - 1));
        } else {
            addRow('', false);
        }

        if (addButton) {
            addButton.addEventListener('click', () => {
                addRow();
            });
        }
    };

    document.addEventListener('DOMContentLoaded', () => {
        document.querySelectorAll('[data-destination-container]').forEach((container) => {
            initDestinationInputs(container);
        });
    });
})();
