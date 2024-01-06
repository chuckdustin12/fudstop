const mobileMenu = document.getElementById('mobile-menu');
const menu = document.querySelector('.navbar-menu');

mobileMenu.addEventListener('click', function() {
    menu.classList.toggle('active');
    this.classList.toggle('is-active');
});



document.querySelectorAll('.option-button').forEach(button => {
    button.addEventListener('click', function(e) {
      const targetId = this.getAttribute('href');
      if (!targetId.startsWith('https://')) {
        e.preventDefault();
        const targetSection = document.querySelector(targetId);
        if (targetSection) {
          targetSection.scrollIntoView({ behavior: 'smooth', block: 'start' });
        }
      }
    });
});

