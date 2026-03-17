# IEEE OpenEI Open Energy Data Initiative (OEDI) Extractor

Python tools for extracting and working with data from the OpenEI Open Energy Data Initiative (OEDI).

## Author

### Tyler Jones

_Senior IEEE Member, Founder & CEO Reliciti_

## Overview

This project is an open-source Python extractor intended to help users discover, download, and process publicly available OEDI/OpenEI datasets. It is designed to be simple, extensible, and suitable for local analysis or integration into larger data workflows.

## Requirements

- Python 3.10+
- pip

## Setup

Clone the repository and create a virtual environment:

```bash
git clone https://github.com/tjones-ieee/ieee-openei-scripts
cd ieee-openei-scripts

python -m venv .venv
```

Activate the virtual environment:

**Windows**

```bash
.venv\Scripts\activate
```

**macOS / Linux**

```bash
source .venv/bin/activate
```

Install dependencies:

```bash
pip install -r requirements.txt
```

If new packages are installed, update requirements

```bash
pip freeze > requirements.txt
```

## Usage

Run the project from the repository root:

```bash
python -m main
```

## Contributing

Contributions, issues, and pull requests are welcome. Please keep changes focused, documented, and aligned with the purpose of the project.

## License

This project is intended to be released as open source under the MIT License.

## Disclaimer

This software is provided **as is**, without warranty of any kind, express or implied, including but not limited to warranties of merchantability, fitness for a particular purpose, and noninfringement.

In no event shall the author, contributors, or affiliated parties be liable for any claim, damages, or other liability, whether in an action of contract, tort, or otherwise, arising from, out of, or in connection with the software or the use of or other dealings in the software.
