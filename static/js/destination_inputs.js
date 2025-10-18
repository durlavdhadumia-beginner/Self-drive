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

        const getRows = () => container.querySelectorAll('[data-role="destination-input-row"]');

        const updateRemoveButtons = () => {
            const rows = getRows();
            rows.forEach((row, index) => {
                const removeBtn = row.querySelector('[data-role="remove-destination"]');
                if (!removeBtn) {
                    return;
                }
                removeBtn.disabled = index === 0 && required && rows.length === 1;
            });
        };

        const createRow = (value = '', isFirst = false) => {
            const row = document.createElement('div');
            row.className = 'input-group destination-input-row';
            row.dataset.role = 'destination-input-row';

            const input = document.createElement('input');
            input.type = 'text';
            input.name = inputName;
            input.className = 'form-control';
            input.placeholder = 'Enter destination';
            input.autocomplete = 'off';
            if (datalistId) {
                input.setAttribute('list', datalistId);
            }
            if (value) {
                input.value = value;
            }
            if (required && isFirst) {
                input.required = true;
            }

            const removeBtn = document.createElement('button');
            removeBtn.type = 'button';
            removeBtn.className = 'btn btn-outline-danger';
            removeBtn.dataset.role = 'remove-destination';
            removeBtn.innerHTML = '<i class="bi bi-x-lg"></i>';
            removeBtn.addEventListener('click', () => {
                if (required && getRows().length === 1) {
                    input.value = '';
                    input.focus();
                    return;
                }
                row.remove();
                updateRemoveButtons();
            });

            row.appendChild(input);
            row.appendChild(removeBtn);
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

        if (initial.length) {
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
