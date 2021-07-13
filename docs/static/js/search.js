var lunrIndex, pagesIndex;

function endsWith(str, suffix) {
    return str.indexOf(suffix, str.length - suffix.length) !== -1;
}

// Initialize lunrjs using our generated index file
function initLunr() {
    if (!endsWith(baseurl,"/")){
        baseurl = baseurl+'/'
    };

    // First retrieve the index file
    $.getJSON(baseurl +"index.json")
        .done(function(index) {
            pagesIndex = index;
            // Set up lunrjs by declaring the fields we use
            // Also provide their boost level for the ranking
            lunrIndex = lunr(function () {
                this.ref('uri');
                this.field('title', {
                    boost: 15
                });
                this.field('keywords', {
                    boost: 12
                });                
                this.field('tags', {
                    boost: 10
                });
                this.field('content', {
                    boost: 5
                });
                this.field('categories');
    
                // Feed lunr with each file and let lunr actually index them
                pagesIndex.forEach(function(page) {            
                    if(!page.uriRel.startsWith('/embeds')) {
                        this.add(page);
                    }                
                }, this);
                
                this.pipeline.remove(this.stemmer);
            })
        })
        .fail(function(jqxhr, textStatus, error) {
            var err = textStatus + ", " + error;
            console.error("Error getting Hugo index file:", err);
        });
}

function getCurrentProductCategory() {
    if(!window.location.pathname) {
        return null;
    }

    var urlParams = window.location.pathname.split('/');
    if(!urlParams || urlParams.length < 3) {
        return null;
    }

    if(urlParams[1] === 'latest') {
        return urlParams[2];
    }

    if(urlParams[1] === 'staging') {
        return urlParams[3];
    }

    return urlParams[1];
}

function isCategorySearchable(cat) {
    if(!cat) {
        return false;
    }

    return ['RS', 'RC', 'RI', 'MODULES', 'PLATFORMS'].includes(cat.toUpperCase());
}

var currentCategory = getCurrentProductCategory();
var isCurrentCategorySearchable = isCategorySearchable(currentCategory);

/**
 * Trigger a search in lunr and transform the result
 *
 * @param  {String} query
 * @return {Array}  results
 */
function search(query) {
    var results = lunrIndex.search(query);
    var r = results.map(function(result) {
        return pagesIndex.filter(function(page) {
            return page.uri === result.ref;
        })[0];
    });
    
    if(!r || r.length < 2 || !isCurrentCategorySearchable) {
        return r;
    }

    var cat = currentCategory.toUpperCase();
    
    r.sort((a, b) => {
        var aCat = (a.categories && a.categories.length > 0)? a.categories[0].toUpperCase() : '';
        var bCat = (b.categories && b.categories.length > 0)? b.categories[0].toUpperCase() : '';

        if(aCat === cat && bCat !== cat) {
            return -1;
        }

        if(aCat !== cat && bCat === cat) {
            return 1;
        }

        return 0;
    });

    return r;
}

// Let's get started
initLunr();
$( document ).ready(function() {
    var searchList = new autoComplete({
        delay: 550,
        /* selector for the search box element */
        selector: $("#search-by").get(0),
        /* source is the callback to perform the search */
        source: function(term, response) {
            response(search(term));
        },
        /* renderItem displays individual search results */
        renderItem: function(item, term) {
            var numContextWords = 3;
            var regEx = "(?:\\s?(?:[\\w\!\"\#\$\%\&\'\(\)\*\+\,\-\.\/\:\;\<\=\>\?\@\[\\\]\^\_\`\{\|\}\~]+)\\s?){0";
            var text = item.content.match(
                regEx+numContextWords+"}" +
                    term+regEx+numContextWords+"}");
            if(text && text.length > 0) {
                var len = text[0].split(' ').length;
                item.context = len > 1? '...' + text[0].trim() + '...' : null;
            }
            item.cat = (item.categories && item.categories.length > 0)? item.categories[0] : '';
            return '<div class="autocomplete-suggestion" ' +
                'data-term="' + term + '" ' +
                'data-title="' + item.title + '" ' +
                'data-uri="'+ item.uri + '?s=' + term + '"' +
                'data-context="' + item.context + '">' +
                    '<div>' + item.title + '<strong class="category">' + item.cat + '</strong> </div>' +
                    '<div class="context">' + (item.context || '') +'</div>' +
                '</div>';
        },
        /* onSelect callback fires when a search suggestion is chosen */
        onSelect: function(e, term, item) {
            console.log(item.getAttribute('data-val'));
            location.href = item.getAttribute('data-uri');
        }
    });
});
