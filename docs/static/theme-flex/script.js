jQuery(document).ready(function() {
    $(window).scroll(function (event) {
        var scroll = $(window).scrollTop();
        var header = $('#header');
        if(scroll > 40) {
            header.css('height', '50px');
        } else {
            header.css('height', '70px');
        }
    });

    $("nav.shortcuts li").hover(
        function() {
            $(this).children('.submenu-wrapper').css('visibility', 'visible');
            $(this).children('.submenu-wrapper').css('opacity', 1);
        }, function() {
            $(this).children('.submenu-wrapper').css('visibility', 'hidden');
            $(this).children('.submenu-wrapper').css('opacity', 0);
        }
    );    

    jQuery('.category-icon').on('click', function() {
        $( this ).toggleClass("fa-angle-down fa-angle-right");
        var x = $( this ).parent().parent().children('ul')
        x.toggle();

        if (x[0].style.display == 'block') {
            x[0].classList.add("menu-ul-expanded");
        } else {
            x[0].classList.remove("menu-ul-expanded");
        }

        setMenuExpansion();
        return false;
    });

    function setMenuExpansion() {
        var $menu = $('article > aside .menu .dd-item.parent.menu-root');
        var ulChildren = $('.parent.menu-root').children('ul').children('li').children('ul');        
        var hasExpandedChildren = false;
        ulChildren.each(function() {
            if($(this).css("display") == "block") {
                hasExpandedChildren = true;
                return false;
            }
        });

        if(hasExpandedChildren > 0) {
            // has expanded children, set to "collapse all"            
            $menu.removeClass('menu-collapsed').addClass('menu-expanded');                                        
            return;
        } else {
            // no expanded children, set to "expand all"
            $menu.removeClass('menu-expanded').addClass('menu-collapsed');            
            return;
        }
    }
        
    jQuery('.SideMenuToggle').on('click', function() {
        var $menu = $('article > aside .menu .dd-item.parent.menu-root');
        if($menu[0].classList.contains('menu-expanded')) {
            // menu is expanded, collapse it
            $('.parent.menu-root').children('ul').find('.fa.category-icon').removeClass('fa-angle-down').addClass('fa-angle-right');
            $menu.removeClass('menu-expanded').addClass('menu-collapsed');
            $('.parent.menu-root').children('ul').find('li').children('ul').toggle(false);
            return false;
        }

        // menu is collapsed, expand it
        $('.parent.menu-root').children('ul').find('.fa.category-icon').removeClass('fa-angle-right').addClass('fa-angle-down');
        $menu.removeClass('menu-collapsed').addClass('menu-expanded');
        $('.parent.menu-root').children('ul').find('li').children('ul').toggle(true);
    });    

    // Images
    // Execute actions on images generated from Markdown pages
    var images = $("article img").not(".inline");
    // Wrap image inside a featherlight (to get a full size view in a popup)
    images.wrap(function () {
        var image = $(this);
        if (!image.parent("a").length) {
            return "<a href='" + image[0].src + "' data-featherlight='image'></a>";
        }
    });
    // Change styles, depending on parameters set to the image
    images.each(function (index) {
        var image = $(this);
        var o = getUrlParameter(image[0].src);
        if (typeof o !== "undefined") {
            var h = o["height"];
            var w = o["width"];
            var c = o["classes"];
            image.css({
                width: function () {
                    if (typeof w !== "undefined") {
                        return w;
                    }
                },
                height: function () {
                    if (typeof h !== "undefined") {
                        return h;
                    }
                }
            });
            if (typeof c !== "undefined") {
                var classes = c.split(',');
                $.each(classes, function(i) {
                    image.addClass(classes[i]);
                });
            }
        }
    });


	// Clipboard
	// Add link button for every
    var text, clip = new Clipboard('.anchor');
    $("h1,h2,h1~h2,h1~h3,h1~h4,h1~h5,h1~h6,h2~h3,h3~h4,h4~h5").append(function (index, html) {
        var element = $(this);
        var url = document.location.origin + document.location.pathname;
        var link = url + "#" + element[0].id;
        return " <span class='anchor' data-clipboard-text='" + link + "'>"  +
            "</span>";
    });

    $(".anchor").on('mouseleave', function (e) {
        $(this).attr('aria-label', null).removeClass('tooltipped tooltipped-s tooltipped-w');
    });

    clip.on('success', function (e) {
        e.clearSelection();
        $(e.trigger).attr('aria-label', 'Link copied to clipboard!').addClass('tooltipped tooltipped-s');
    });


    var ajax;
    jQuery('[data-search-input]').on('input', function() {
        var input = jQuery(this),
            value = input.val(),
            items = jQuery('[data-nav-id]');
        items.removeClass('search-match');
        if (!value.length) {
            $('ul.menu').removeClass('searched');
            items.css('display', 'block');
            sessionStorage.removeItem('search-value');
            return;
        }

        sessionStorage.setItem('search-value', value);

        if (ajax && ajax.abort) ajax.abort();

        jQuery('[data-search-clear]').on('click', function() {
            jQuery('[data-search-input]').val('').trigger('input');
            sessionStorage.removeItem('search-input');
        });
    });

    $.expr[":"].contains = $.expr.createPseudo(function(arg) {
        return function( elem ) {
            return $(elem).text().toUpperCase().indexOf(arg.toUpperCase()) >= 0;
        };
    });

    if (sessionStorage.getItem('search-value')) {
        var searchValue = sessionStorage.getItem('search-value')
        sessionStorage.removeItem('search-value');
        var searchedElem = $('article').find(':contains(' + searchValue + ')').get(0);
        searchedElem && searchedElem.scrollIntoView();
    }

    // clipboard
    var clipInit = false;
    $('pre code').each(function() {
        var code = $(this),
            text = code.text();

        if (text.length > 5) {
            if (!clipInit) {
                var text, clip = new Clipboard('.copy-to-clipboard', {
                    text: function(trigger) {
                        text = $(trigger).next('code').text();
                        return text.replace(/^\$\s/gm, '');
                    }
                });

                var inPre;
                clip.on('success', function(e) {
                    e.clearSelection();
                    inPre = $(e.trigger).parent().prop('tagName') == 'PRE';
                    $(e.trigger).attr('aria-label', 'Copied to clipboard!').addClass('tooltipped tooltipped-' + (inPre ? 'w' : 's'));
                });

                clip.on('error', function(e) {
                    inPre = $(e.trigger).parent().prop('tagName') == 'PRE';
                    $(e.trigger).attr('aria-label', fallbackMessage(e.action)).addClass('tooltipped tooltipped-' + (inPre ? 'w' : 's'));
                    $(document).one('copy', function(){
                        $(e.trigger).attr('aria-label', 'Copied to clipboard!').addClass('tooltipped tooltipped-' + (inPre ? 'w' : 's'));
                    });
                });

                clipInit = true;
            }

            code.before('<span class="copy-to-clipboard" title="Copy to clipboard"><span class="icon-icons_Copy" aria-hidden="true"></span></span>');
            $('.copy-to-clipboard').on('mouseleave', function() {
                $(this).attr('aria-label', null).removeClass('tooltipped tooltipped-s tooltipped-w');
            });
        }
    });

    // allow keyboard control for prev/next links
    jQuery(function() {
        jQuery('.nav-prev').click(function(){
            location.href = jQuery(this).attr('href');
        });
        jQuery('.nav-next').click(function() {
            location.href = jQuery(this).attr('href');
        });
    });
    jQuery(document).keydown(function(e) {
      // prev links - left arrow key
      if(e.which == '37') {
        jQuery('.nav.nav-prev').click();
      }
      // next links - right arrow key
      if(e.which == '39') {
        jQuery('.nav.nav-next').click();
      }
    });

    $('article a:not(:has(img)):not(.btn)').addClass('highlight');
});

$(function() {
    $('a[rel="lightbox"]').featherlight({
        root: 'article'
    });
});

// Get Parameters from some url
var getUrlParameter = function getUrlParameter(sPageURL) {
    var url = sPageURL.split('?');
    var obj = {};
    if (url.length == 2) {
        var sURLVariables = url[1].split('&'),
            sParameterName,
            i;
        for (i = 0; i < sURLVariables.length; i++) {
            sParameterName = sURLVariables[i].split('=');
            obj[sParameterName[0]] = sParameterName[1];
        }
        return obj;
    } else {
        return undefined;
    }
};

function setPageFeedback(kind, pageTitle, category) {
    setFeedbackMessage(kind, category, pageTitle);

    if (window.ga) {
        window.ga('send', {
            hitType: 'event',
            eventCategory: 'Article feedback',
            eventAction: 'vote',
            eventLabel: window.location.pathname,
            eventValue: kind,
            hitCallback: function() {
                setFeedbackMessage();
            }
        });
    }
}

function setFeedbackMessage(kind, category, pageTitle) {
    if(kind === 1) {
        $('#page-feedback-open').html('Thanks for the feedback!');
    } else {
        $('#page-feedback-open')
        .html('Thanks for your feedback! You can help improve the page by opening a <a style="text-decoration: underline;cursor:pointer;" href="https://github.com/vectorizedio/redpanda/issues/new?assignees=&labels=&template=documentation-bug-report.md&title='+ category + ' - ' + pageTitle +'" target="_blank" rel="noopener noreferrer">GitHub issue</a>.');
    }
    setTimeout(function() {
        $('.page-feedback').hide();
    }, 5000);
}

function hidePageFeedback() {
    $('#page-feedback-open').hide();
    $('#page-feedback-closed').show();
    localStorage.setItem('is-page-feedback-visible', 'no');
}

function showPageFeedback() {
    $('#page-feedback-closed').hide();
    $('#page-feedback-open').show();
    localStorage.setItem('is-page-feedback-visible', 'yes');
}

$(function() {
    $('.page-feedback').show();
    var isPageFeedbackVisible = localStorage.getItem('is-page-feedback-visible');
    if(isPageFeedbackVisible == 'no') {
        hidePageFeedback();
    } else {
        showPageFeedback();
    }
});

// Expand and collapse control for expand shortcode
function toggleExpand(el) {
    if(!el || !el.parentElement || el.parentElement.className !== 'expand-parent') {
        return;
    }

    var newIconClass, newLabel = null;
    if(el.children[1].innerText === 'Expand all') {
        newIconClass = 'fa fa-angle-double-up fa-lg';
        newLabel = 'Collapse all';
    } else {
        newIconClass = 'fa fa-angle-double-down fa-lg';
        newLabel = 'Expand all';
    }

    el.children[0].className = newIconClass; // Update the root control icon
    el.children[1].innerText = newLabel; // Update the text of root control label
    
    var exNodes = el.parentElement.getElementsByClassName('expand');
    if(!exNodes || exNodes.length === 0) {
        return;
    }    

    var exNodesArr = Array.from(exNodes);

    // Toggle expand items and update their icons
    exNodesArr.forEach(function(element) {
        var elChildren = element.children;
        if(!elChildren || elChildren.length === 0) {
            return;
        }

        // Update item control icon
        if(elChildren[0] && elChildren[0].children && elChildren[0].children[0]) {
            elChildren[0].children[0].className = (newLabel === 'Collapse all')? 'icon-icons_minus': 'icon-icons_plus';
        }

        // Update item control label
        if(elChildren[1]) {
            elChildren[1].style.display = (newLabel === 'Collapse all')? 'block' : 'none';
        }
    });    
}

function handleToggleExpandItem(el) {    
    if(!el || !el.parentElement || !el.parentElement.parentElement || el.parentElement.parentElement.className !== 'expand-parent') {
        return;
    }

    var exNodes = el.parentElement.parentElement.getElementsByClassName('expand');
    if(!exNodes || exNodes.length === 0) {
        return;
    }    
    
    var exNodesArr = Array.from(exNodes);
    var totalCount = exNodesArr.length;
    var expandedCount = 0;
    
    // Count the number of expanded items
    exNodesArr.forEach(function(element) {
        var elChildren = element.children;
        if(!elChildren || elChildren.length === 0) {
            return;
        }

        if(elChildren[1] && elChildren[1].style.display !== 'none') {
            expandedCount++;
        }
    });

    var iconClass, label = null;
    if(expandedCount === totalCount) {
        iconClass = 'fa fa-angle-double-up fa-lg';
        label = 'Collapse all';
    }
    if(expandedCount === 0) {
        iconClass = 'fa fa-angle-double-down fa-lg';
        label = 'Expand all';
    }
    
    if(label) {
        var elControl = el.parentElement.parentElement.querySelector('.expand-shortcode-control');
        if(!elControl) {
            return;
        }

        elControl.children[0].className = iconClass;
        elControl.children[1].innerText = label;
    }
}

$('#nav-tab a').on('click', function(e) {
    e.preventDefault();
    $(this).tab('show');
    var T = $(this);
    $('#nav-tab a').removeClass('active');
    T.addClass('active');
});
