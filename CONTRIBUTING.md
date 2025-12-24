# Contributing to PySpark Learning Hub

Thank you for your interest in contributing to the PySpark Learning Hub! This document provides guidelines for contributing to this educational repository.

## Code of Conduct

Please read and follow our [Code of Conduct](CODE_OF_CONDUCT.md) to maintain a welcoming and inclusive environment.

## How to Contribute

### Reporting Issues

- Check existing issues before creating a new one
- Provide clear, detailed descriptions
- Include steps to reproduce for bugs
- Suggest improvements with examples

### Suggesting Enhancements

- Clearly describe the enhancement
- Explain why it would be useful
- Provide examples or mockups if applicable

### Contributing Code

1. **Fork the Repository**
   ```bash
   git clone https://github.com/sunkaramallikarjuna369/PySpark.git
   cd PySpark
   ```

2. **Create a Branch**
   ```bash
   git checkout -b feature/your-feature-name
   ```

3. **Make Your Changes**
   - Follow existing code style and conventions
   - Add comments where necessary
   - Update documentation if needed

4. **Test Your Changes**
   - Verify all code examples work
   - Test visualizations in browser
   - Check responsive design

5. **Commit Your Changes**
   ```bash
   git add .
   git commit -m "Description of changes"
   ```

6. **Push and Create Pull Request**
   ```bash
   git push origin feature/your-feature-name
   ```

## Contribution Guidelines

### Code Style

- **Python/PySpark**: Follow PEP 8 style guide
- **JavaScript**: Use ES6+ syntax, consistent indentation
- **HTML/CSS**: Semantic HTML, organized CSS
- **Comments**: Clear, concise, helpful

### Documentation

- Update README.md for major changes
- Add inline comments for complex logic
- Include examples for new features
- Keep documentation up-to-date

### Concept Structure

Each concept folder should contain:
- `index.html` - Interactive visualization and code examples
- `{concept_name}.py` - PySpark code demonstrations
- `README.md` - Detailed explanation (optional)

### Adding New Concepts

1. Create folder: `{category}/{number}_{concept_name}/`
2. Add HTML with 3D visualization
3. Add Python/PySpark code examples
4. Update main `index.html` navigation
5. Test all functionality

### Sample Data

- Use realistic, representative data
- Include CSV files in `data/` directory
- Document data structure in `data/README.md`
- Ensure data privacy (no real PII)

## Quality Standards

### Code Quality

- ✅ Code runs without errors
- ✅ Examples are clear and educational
- ✅ Follows existing patterns
- ✅ Properly commented

### Visualization Quality

- ✅ 3D visualizations load correctly
- ✅ Interactive controls work
- ✅ Responsive design (mobile + desktop)
- ✅ Dark/light theme support

### Documentation Quality

- ✅ Clear explanations
- ✅ Correct technical information
- ✅ Proper grammar and spelling
- ✅ Helpful examples

## Review Process

1. Submit pull request
2. Automated checks run
3. Code review by maintainers
4. Address feedback
5. Approval and merge

## Getting Help

- Open an issue for questions
- Check existing documentation
- Review similar concepts for examples

## Recognition

Contributors will be recognized in:
- Repository contributors list
- Release notes
- Special thanks section

## License

By contributing, you agree that your contributions will be licensed under the MIT License.

---

Thank you for helping make PySpark learning better for everyone!
