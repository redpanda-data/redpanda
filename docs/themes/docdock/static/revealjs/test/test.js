
// These tests expect the DOM to contain a presentation
// with the following slide structure:
//
// 1
// 2 - Three sub-slides
// 3 - Three fragment elements
// 3 - Two fragments with same data-fragment-index
// 4


Reveal.addEventListener( 'ready', function() {

	// ---------------------------------------------------------------
	// DOM TESTS

	QUnit.module( 'DOM' );

	test( 'Initial slides classes', function() {
		var horizontalSlides = document.querySelectorAll( '.reveal .slides>section' )

		strictEqual( document.querySelectorAll( '.reveal .slides section.past' ).length, 0, 'no .past slides' );
		strictEqual( document.querySelectorAll( '.reveal .slides section.present' ).length, 1, 'one .present slide' );
		strictEqual( document.querySelectorAll( '.reveal .slides>section.future' ).length, horizontalSlides.length - 1, 'remaining horizontal slides are .future' );

		strictEqual( document.querySelectorAll( '.reveal .slides section.stack' ).length, 2, 'two .stacks' );

		ok( document.querySelectorAll( '.reveal .slides section.stack' )[0].querySelectorAll( '.future' ).length > 0, 'vertical slides are given .future' );
	});

	// ---------------------------------------------------------------
	// API TESTS

	QUnit.module( 'API' );

	test( 'Reveal.isReady', function() {
		strictEqual( Reveal.isReady(), true, 'returns true' );
	});

	test( 'Reveal.isOverview', function() {
		strictEqual( Reveal.isOverview(), false, 'false by default' );

		Reveal.toggleOverview();
		strictEqual( Reveal.isOverview(), true, 'true after toggling on' );

		Reveal.toggleOverview();
		strictEqual( Reveal.isOverview(), false, 'false after toggling off' );
	});

	test( 'Reveal.isPaused', function() {
		strictEqual( Reveal.isPaused(), false, 'false by default' );

		Reveal.togglePause();
		strictEqual( Reveal.isPaused(), true, 'true after pausing' );

		Reveal.togglePause();
		strictEqual( Reveal.isPaused(), false, 'false after resuming' );
	});

	test( 'Reveal.isFirstSlide', function() {
		Reveal.slide( 0, 0 );
		strictEqual( Reveal.isFirstSlide(), true, 'true after Reveal.slide( 0, 0 )' );

		Reveal.slide( 1, 0 );
		strictEqual( Reveal.isFirstSlide(), false, 'false after Reveal.slide( 1, 0 )' );

		Reveal.slide( 0, 0 );
		strictEqual( Reveal.isFirstSlide(), true, 'true after Reveal.slide( 0, 0 )' );
	});

	test( 'Reveal.isFirstSlide after vertical slide', function() {
		Reveal.slide( 1, 1 );
		Reveal.slide( 0, 0 );
		strictEqual( Reveal.isFirstSlide(), true, 'true after Reveal.slide( 1, 1 ) and then Reveal.slide( 0, 0 )' );
	});

	test( 'Reveal.isLastSlide', function() {
		Reveal.slide( 0, 0 );
		strictEqual( Reveal.isLastSlide(), false, 'false after Reveal.slide( 0, 0 )' );

		var lastSlideIndex = document.querySelectorAll( '.reveal .slides>section' ).length - 1;

		Reveal.slide( lastSlideIndex, 0 );
		strictEqual( Reveal.isLastSlide(), true, 'true after Reveal.slide( '+ lastSlideIndex +', 0 )' );

		Reveal.slide( 0, 0 );
		strictEqual( Reveal.isLastSlide(), false, 'false after Reveal.slide( 0, 0 )' );
	});

	test( 'Reveal.isLastSlide after vertical slide', function() {
		var lastSlideIndex = document.querySelectorAll( '.reveal .slides>section' ).length - 1;

		Reveal.slide( 1, 1 );
		Reveal.slide( lastSlideIndex );
		strictEqual( Reveal.isLastSlide(), true, 'true after Reveal.slide( 1, 1 ) and then Reveal.slide( '+ lastSlideIndex +', 0 )' );
	});

	test( 'Reveal.getTotalSlides', function() {
		strictEqual( Reveal.getTotalSlides(), 8, 'eight slides in total' );
	});

	test( 'Reveal.getIndices', function() {
		var indices = Reveal.getIndices();

		ok( indices.hasOwnProperty( 'h' ), 'h exists' );
		ok( indices.hasOwnProperty( 'v' ), 'v exists' );
		ok( indices.hasOwnProperty( 'f' ), 'f exists' );

		Reveal.slide( 1, 0 );
		strictEqual( Reveal.getIndices().h, 1, 'h 1' );
		strictEqual( Reveal.getIndices().v, 0, 'v 0' );

		Reveal.slide( 1, 2 );
		strictEqual( Reveal.getIndices().h, 1, 'h 1' );
		strictEqual( Reveal.getIndices().v, 2, 'v 2' );

		Reveal.slide( 0, 0 );
		strictEqual( Reveal.getIndices().h, 0, 'h 0' );
		strictEqual( Reveal.getIndices().v, 0, 'v 0' );
	});

	test( 'Reveal.getSlide', function() {
		equal( Reveal.getSlide( 0 ), document.querySelector( '.reveal .slides>section:first-child' ), 'gets correct first slide' );
		equal( Reveal.getSlide( 1 ), document.querySelector( '.reveal .slides>section:nth-child(2)' ), 'no v index returns stack' );
		equal( Reveal.getSlide( 1, 0 ), document.querySelector( '.reveal .slides>section:nth-child(2)>section:nth-child(1)' ), 'v index 0 returns first vertical child' );
		equal( Reveal.getSlide( 1, 1 ), document.querySelector( '.reveal .slides>section:nth-child(2)>section:nth-child(2)' ), 'v index 1 returns second vertical child' );

		strictEqual( Reveal.getSlide( 100 ), undefined, 'undefined when out of horizontal bounds' );
		strictEqual( Reveal.getSlide( 1, 100 ), undefined, 'undefined when out of vertical bounds' );
	});

	test( 'Reveal.getSlideBackground', function() {
		equal( Reveal.getSlideBackground( 0 ), document.querySelector( '.reveal .backgrounds>.slide-background:first-child' ), 'gets correct first background' );
		equal( Reveal.getSlideBackground( 1 ), document.querySelector( '.reveal .backgrounds>.slide-background:nth-child(2)' ), 'no v index returns stack' );
		equal( Reveal.getSlideBackground( 1, 0 ), document.querySelector( '.reveal .backgrounds>.slide-background:nth-child(2) .slide-background:nth-child(1)' ), 'v index 0 returns first vertical child' );
		equal( Reveal.getSlideBackground( 1, 1 ), document.querySelector( '.reveal .backgrounds>.slide-background:nth-child(2) .slide-background:nth-child(2)' ), 'v index 1 returns second vertical child' );

		strictEqual( Reveal.getSlideBackground( 100 ), undefined, 'undefined when out of horizontal bounds' );
		strictEqual( Reveal.getSlideBackground( 1, 100 ), undefined, 'undefined when out of vertical bounds' );
	});

	test( 'Reveal.getSlideNotes', function() {
		Reveal.slide( 0, 0 );
		ok( Reveal.getSlideNotes() === 'speaker notes 1', 'works with <aside class="notes">' );

		Reveal.slide( 1, 0 );
		ok( Reveal.getSlideNotes() === 'speaker notes 2', 'works with <section data-notes="">' );
	});

	test( 'Reveal.getPreviousSlide/getCurrentSlide', function() {
		Reveal.slide( 0, 0 );
		Reveal.slide( 1, 0 );

		var firstSlide = document.querySelector( '.reveal .slides>section:first-child' );
		var secondSlide = document.querySelector( '.reveal .slides>section:nth-child(2)>section' );

		equal( Reveal.getPreviousSlide(), firstSlide, 'previous is slide #0' );
		equal( Reveal.getCurrentSlide(), secondSlide, 'current is slide #1' );
	});

	test( 'Reveal.getProgress', function() {
		Reveal.slide( 0, 0 );
		strictEqual( Reveal.getProgress(), 0, 'progress is 0 on first slide' );

		var lastSlideIndex = document.querySelectorAll( '.reveal .slides>section' ).length - 1;

		Reveal.slide( lastSlideIndex, 0 );
		strictEqual( Reveal.getProgress(), 1, 'progress is 1 on last slide' );
	});

	test( 'Reveal.getScale', function() {
		ok( typeof Reveal.getScale() === 'number', 'has scale' );
	});

	test( 'Reveal.getConfig', function() {
		ok( typeof Reveal.getConfig() === 'object', 'has config' );
	});

	test( 'Reveal.configure', function() {
		strictEqual( Reveal.getConfig().loop, false, '"loop" is false to start with' );

		Reveal.configure({ loop: true });
		strictEqual( Reveal.getConfig().loop, true, '"loop" has changed to true' );

		Reveal.configure({ loop: false, customTestValue: 1 });
		strictEqual( Reveal.getConfig().customTestValue, 1, 'supports custom values' );
	});

	test( 'Reveal.availableRoutes', function() {
		Reveal.slide( 0, 0 );
		deepEqual( Reveal.availableRoutes(), { left: false, up: false, down: false, right: true }, 'correct for first slide' );

		Reveal.slide( 1, 0 );
		deepEqual( Reveal.availableRoutes(), { left: true, up: false, down: true, right: true }, 'correct for vertical slide' );
	});

	test( 'Reveal.next', function() {
		Reveal.slide( 0, 0 );

		// Step through vertical child slides
		Reveal.next();
		deepEqual( Reveal.getIndices(), { h: 1, v: 0, f: undefined } );

		Reveal.next();
		deepEqual( Reveal.getIndices(), { h: 1, v: 1, f: undefined } );

		Reveal.next();
		deepEqual( Reveal.getIndices(), { h: 1, v: 2, f: undefined } );

		// Step through fragments
		Reveal.next();
		deepEqual( Reveal.getIndices(), { h: 2, v: 0, f: -1 } );

		Reveal.next();
		deepEqual( Reveal.getIndices(), { h: 2, v: 0, f: 0 } );

		Reveal.next();
		deepEqual( Reveal.getIndices(), { h: 2, v: 0, f: 1 } );

		Reveal.next();
		deepEqual( Reveal.getIndices(), { h: 2, v: 0, f: 2 } );
	});

	test( 'Reveal.next at end', function() {
		Reveal.slide( 3 );

		// We're at the end, this should have no effect
		Reveal.next();
		deepEqual( Reveal.getIndices(), { h: 3, v: 0, f: undefined } );

		Reveal.next();
		deepEqual( Reveal.getIndices(), { h: 3, v: 0, f: undefined } );
	});


	// ---------------------------------------------------------------
	// FRAGMENT TESTS

	QUnit.module( 'Fragments' );

	test( 'Sliding to fragments', function() {
		Reveal.slide( 2, 0, -1 );
		deepEqual( Reveal.getIndices(), { h: 2, v: 0, f: -1 }, 'Reveal.slide( 2, 0, -1 )' );

		Reveal.slide( 2, 0, 0 );
		deepEqual( Reveal.getIndices(), { h: 2, v: 0, f: 0 }, 'Reveal.slide( 2, 0, 0 )' );

		Reveal.slide( 2, 0, 2 );
		deepEqual( Reveal.getIndices(), { h: 2, v: 0, f: 2 }, 'Reveal.slide( 2, 0, 2 )' );

		Reveal.slide( 2, 0, 1 );
		deepEqual( Reveal.getIndices(), { h: 2, v: 0, f: 1 }, 'Reveal.slide( 2, 0, 1 )' );
	});

	test( 'Hiding all fragments', function() {
		var fragmentSlide = document.querySelector( '#fragment-slides>section:nth-child(1)' );

		Reveal.slide( 2, 0, 0 );
		strictEqual( fragmentSlide.querySelectorAll( '.fragment.visible' ).length, 1, 'one fragment visible when index is 0' );

		Reveal.slide( 2, 0, -1 );
		strictEqual( fragmentSlide.querySelectorAll( '.fragment.visible' ).length, 0, 'no fragments visible when index is -1' );
	});

	test( 'Current fragment', function() {
		var fragmentSlide = document.querySelector( '#fragment-slides>section:nth-child(1)' );

		Reveal.slide( 2, 0 );
		strictEqual( fragmentSlide.querySelectorAll( '.fragment.current-fragment' ).length, 0, 'no current fragment at index -1' );

		Reveal.slide( 2, 0, 0 );
		strictEqual( fragmentSlide.querySelectorAll( '.fragment.current-fragment' ).length, 1, 'one current fragment at index 0' );

		Reveal.slide( 1, 0, 0 );
		strictEqual( fragmentSlide.querySelectorAll( '.fragment.current-fragment' ).length, 0, 'no current fragment when navigating to previous slide' );

		Reveal.slide( 3, 0, 0 );
		strictEqual( fragmentSlide.querySelectorAll( '.fragment.current-fragment' ).length, 0, 'no current fragment when navigating to next slide' );
	});

	test( 'Stepping through fragments', function() {
		Reveal.slide( 2, 0, -1 );

		// forwards:

		Reveal.next();
		deepEqual( Reveal.getIndices(), { h: 2, v: 0, f: 0 }, 'next() goes to next fragment' );

		Reveal.right();
		deepEqual( Reveal.getIndices(), { h: 2, v: 0, f: 1 }, 'right() goes to next fragment' );

		Reveal.down();
		deepEqual( Reveal.getIndices(), { h: 2, v: 0, f: 2 }, 'down() goes to next fragment' );

		Reveal.down(); // moves to f #3

		// backwards:

		Reveal.prev();
		deepEqual( Reveal.getIndices(), { h: 2, v: 0, f: 2 }, 'prev() goes to prev fragment' );

		Reveal.left();
		deepEqual( Reveal.getIndices(), { h: 2, v: 0, f: 1 }, 'left() goes to prev fragment' );

		Reveal.up();
		deepEqual( Reveal.getIndices(), { h: 2, v: 0, f: 0 }, 'up() goes to prev fragment' );
	});

	test( 'Stepping past fragments', function() {
		var fragmentSlide = document.querySelector( '#fragment-slides>section:nth-child(1)' );

		Reveal.slide( 0, 0, 0 );
		equal( fragmentSlide.querySelectorAll( '.fragment.visible' ).length, 0, 'no fragments visible when on previous slide' );

		Reveal.slide( 3, 0, 0 );
		equal( fragmentSlide.querySelectorAll( '.fragment.visible' ).length, 3, 'all fragments visible when on future slide' );
	});

	test( 'Fragment indices', function() {
		var fragmentSlide = document.querySelector( '#fragment-slides>section:nth-child(2)' );

		Reveal.slide( 3, 0, 0 );
		equal( fragmentSlide.querySelectorAll( '.fragment.visible' ).length, 2, 'both fragments of same index are shown' );

		// This slide has three fragments, first one is index 0, second and third have index 1
		Reveal.slide( 2, 2, 0 );
		equal( Reveal.getIndices().f, 0, 'returns correct index for first fragment' );

		Reveal.slide( 2, 2, 1 );
		equal( Reveal.getIndices().f, 1, 'returns correct index for two fragments with same index' );
	});

	test( 'Index generation', function() {
		var fragmentSlide = document.querySelector( '#fragment-slides>section:nth-child(1)' );

		// These have no indices defined to start with
		equal( fragmentSlide.querySelectorAll( '.fragment' )[0].getAttribute( 'data-fragment-index' ), '0' );
		equal( fragmentSlide.querySelectorAll( '.fragment' )[1].getAttribute( 'data-fragment-index' ), '1' );
		equal( fragmentSlide.querySelectorAll( '.fragment' )[2].getAttribute( 'data-fragment-index' ), '2' );
	});

	test( 'Index normalization', function() {
		var fragmentSlide = document.querySelector( '#fragment-slides>section:nth-child(3)' );

		// These start out as 1-4-4 and should normalize to 0-1-1
		equal( fragmentSlide.querySelectorAll( '.fragment' )[0].getAttribute( 'data-fragment-index' ), '0' );
		equal( fragmentSlide.querySelectorAll( '.fragment' )[1].getAttribute( 'data-fragment-index' ), '1' );
		equal( fragmentSlide.querySelectorAll( '.fragment' )[2].getAttribute( 'data-fragment-index' ), '1' );
	});

	asyncTest( 'fragmentshown event', function() {
		expect( 2 );

		var _onEvent = function( event ) {
			ok( true, 'event fired' );
		}

		Reveal.addEventListener( 'fragmentshown', _onEvent );

		Reveal.slide( 2, 0 );
		Reveal.slide( 2, 0 ); // should do nothing
		Reveal.slide( 2, 0, 0 ); // should do nothing
		Reveal.next();
		Reveal.next();
		Reveal.prev(); // shouldn't fire fragmentshown

		start();

		Reveal.removeEventListener( 'fragmentshown', _onEvent );
	});

	asyncTest( 'fragmenthidden event', function() {
		expect( 2 );

		var _onEvent = function( event ) {
			ok( true, 'event fired' );
		}

		Reveal.addEventListener( 'fragmenthidden', _onEvent );

		Reveal.slide( 2, 0, 2 );
		Reveal.slide( 2, 0, 2 ); // should do nothing
		Reveal.prev();
		Reveal.prev();
		Reveal.next(); // shouldn't fire fragmenthidden

		start();

		Reveal.removeEventListener( 'fragmenthidden', _onEvent );
	});


	// ---------------------------------------------------------------
	// AUTO-SLIDE TESTS

	QUnit.module( 'Auto Sliding' );

	test( 'Reveal.isAutoSliding', function() {
		strictEqual( Reveal.isAutoSliding(), false, 'false by default' );

		Reveal.configure({ autoSlide: 10000 });
		strictEqual( Reveal.isAutoSliding(), true, 'true after starting' );

		Reveal.configure({ autoSlide: 0 });
		strictEqual( Reveal.isAutoSliding(), false, 'false after setting to 0' );
	});

	test( 'Reveal.toggleAutoSlide', function() {
		Reveal.configure({ autoSlide: 10000 });

		Reveal.toggleAutoSlide();
		strictEqual( Reveal.isAutoSliding(), false, 'false after first toggle' );
		Reveal.toggleAutoSlide();
		strictEqual( Reveal.isAutoSliding(), true, 'true after second toggle' );

		Reveal.configure({ autoSlide: 0 });
	});

	asyncTest( 'autoslidepaused', function() {
		expect( 1 );

		var _onEvent = function( event ) {
			ok( true, 'event fired' );
		}

		Reveal.addEventListener( 'autoslidepaused', _onEvent );
		Reveal.configure({ autoSlide: 10000 });
		Reveal.toggleAutoSlide();

		start();

		// cleanup
		Reveal.configure({ autoSlide: 0 });
		Reveal.removeEventListener( 'autoslidepaused', _onEvent );
	});

	asyncTest( 'autoslideresumed', function() {
		expect( 1 );

		var _onEvent = function( event ) {
			ok( true, 'event fired' );
		}

		Reveal.addEventListener( 'autoslideresumed', _onEvent );
		Reveal.configure({ autoSlide: 10000 });
		Reveal.toggleAutoSlide();
		Reveal.toggleAutoSlide();

		start();

		// cleanup
		Reveal.configure({ autoSlide: 0 });
		Reveal.removeEventListener( 'autoslideresumed', _onEvent );
	});


	// ---------------------------------------------------------------
	// CONFIGURATION VALUES

	QUnit.module( 'Configuration' );

	test( 'Controls', function() {
		var controlsElement = document.querySelector( '.reveal>.controls' );

		Reveal.configure({ controls: false });
		equal( controlsElement.style.display, 'none', 'controls are hidden' );

		Reveal.configure({ controls: true });
		equal( controlsElement.style.display, 'block', 'controls are visible' );
	});

	test( 'Progress', function() {
		var progressElement = document.querySelector( '.reveal>.progress' );

		Reveal.configure({ progress: false });
		equal( progressElement.style.display, 'none', 'progress are hidden' );

		Reveal.configure({ progress: true });
		equal( progressElement.style.display, 'block', 'progress are visible' );
	});

	test( 'Loop', function() {
		Reveal.configure({ loop: true });

		Reveal.slide( 0, 0 );

		Reveal.left();
		notEqual( Reveal.getIndices().h, 0, 'looped from start to end' );

		Reveal.right();
		equal( Reveal.getIndices().h, 0, 'looped from end to start' );

		Reveal.configure({ loop: false });
	});


	// ---------------------------------------------------------------
	// LAZY-LOADING TESTS

	QUnit.module( 'Lazy-Loading' );

	test( 'img with data-src', function() {
		strictEqual( document.querySelectorAll( '.reveal section img[src]' ).length, 1, 'Image source has been set' );
	});

	test( 'video with data-src', function() {
		strictEqual( document.querySelectorAll( '.reveal section video[src]' ).length, 1, 'Video source has been set' );
	});

	test( 'audio with data-src', function() {
		strictEqual( document.querySelectorAll( '.reveal section audio[src]' ).length, 1, 'Audio source has been set' );
	});

	test( 'iframe with data-src', function() {
		Reveal.slide( 0, 0 );
		strictEqual( document.querySelectorAll( '.reveal section iframe[src]' ).length, 0, 'Iframe source is not set' );
		Reveal.slide( 2, 1 );
		strictEqual( document.querySelectorAll( '.reveal section iframe[src]' ).length, 1, 'Iframe source is set' );
		Reveal.slide( 2, 2 );
		strictEqual( document.querySelectorAll( '.reveal section iframe[src]' ).length, 0, 'Iframe source is not set' );
	});

	test( 'background images', function() {
		var imageSource1 = Reveal.getSlide( 0 ).getAttribute( 'data-background-image' );
		var imageSource2 = Reveal.getSlide( 1, 0 ).getAttribute( 'data-background' );

		// check that the images are applied to the background elements
		ok( Reveal.getSlideBackground( 0 ).style.backgroundImage.indexOf( imageSource1 ) !== -1, 'data-background-image worked' );
		ok( Reveal.getSlideBackground( 1, 0 ).style.backgroundImage.indexOf( imageSource2 ) !== -1, 'data-background worked' );
	});


	// ---------------------------------------------------------------
	// EVENT TESTS

	QUnit.module( 'Events' );

	asyncTest( 'slidechanged', function() {
		expect( 3 );

		var _onEvent = function( event ) {
			ok( true, 'event fired' );
		}

		Reveal.addEventListener( 'slidechanged', _onEvent );

		Reveal.slide( 1, 0 ); // should trigger
		Reveal.slide( 1, 0 ); // should do nothing
		Reveal.next(); // should trigger
		Reveal.slide( 3, 0 ); // should trigger
		Reveal.next(); // should do nothing

		start();

		Reveal.removeEventListener( 'slidechanged', _onEvent );

	});

	asyncTest( 'paused', function() {
		expect( 1 );

		var _onEvent = function( event ) {
			ok( true, 'event fired' );
		}

		Reveal.addEventListener( 'paused', _onEvent );

		Reveal.togglePause();
		Reveal.togglePause();

		start();

		Reveal.removeEventListener( 'paused', _onEvent );
	});

	asyncTest( 'resumed', function() {
		expect( 1 );

		var _onEvent = function( event ) {
			ok( true, 'event fired' );
		}

		Reveal.addEventListener( 'resumed', _onEvent );

		Reveal.togglePause();
		Reveal.togglePause();

		start();

		Reveal.removeEventListener( 'resumed', _onEvent );
	});


} );

Reveal.initialize();

